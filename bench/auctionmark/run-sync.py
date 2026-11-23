#!/usr/bin/env python3
"""Run a disposable RustFS-backed benchmark with separate server and client limits."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import string
import subprocess
import sys
import time
import tomllib
import uuid

RUSTFS_IMAGE = "rustfs/rustfs:1.0.0"
RC_IMAGE = "rustfs/rc:v0.1.36"
BENCH = Path(__file__).resolve().parent
REPO = BENCH.parent.parent


def run(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def output(*args):
    return run(*args, stdout=subprocess.PIPE).stdout.strip()


def stop(process):
    if process is not None and process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def wait_port(port, process=None, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError("Server exited during startup; inspect server.log")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"Port {port} did not become ready")


def free_ports(count):
    # Hold all sockets open together so the ports are distinct.
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def interrupted(signum, _frame):
    raise InterruptedError(f"Received signal {signum}")


def benchmark(args, benchmark_args):
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, interrupted)
    directory = Path(args.output_dir).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    binary = Path(args.server_binary).resolve()
    if not binary.is_file():
        raise FileNotFoundError(f"Build the release server first: {binary}")
    name = "auctionmark-sync-" + uuid.uuid4().hex[:12]
    rc_name = name + "-setup"
    server = client = None
    with binary.open("rb") as executable:
        digest = hashlib.file_digest(executable, "sha256").hexdigest()
    rustfs_port, port = free_ports(2)
    config_path = directory / "server.toml"
    config_path.write_text(string.Template((BENCH / "server.toml").read_text()).substitute(
        rustfs_port=rustfs_port, port=port,
        cache_path=json.dumps(str(directory / "cache")),
        log_path=json.dumps(str(directory / "log" / "log"))))
    config = tomllib.loads(config_path.read_text())
    storage, incremental = config["storage"], config["incremental"]
    # The server has no memory limit. Its DBSP RSS budget is 75% of host RAM.
    server_rss_mib = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // 4 * 3 // 2**20
    metadata = {
        "runtime": "pooled", "foreground_threads": incremental["threads"],
        "merger_threads": incremental["merger_threads"],
        "shared_cache_mib": incremental["cache_mib"], "cdc_poll_ms": 200,
        "remote_flush_us": storage["wal_flush_interval_us"],
        "storage": "RustFS", "log": "local file",
        "rustfs_image": RUSTFS_IMAGE, "server_binary": str(binary),
        "server_sha256": digest,
        "server_cpu_quota": None, "server_memory_mib": None,
        "client_cpu_quota": 2.0, "client_memory_mib": 2048,
        "combined_memory_mib": server_rss_mib + 2048 + 768,
        "rustfs_cpu_quota": 0.25, "rustfs_memory_mib": 768,
        "revision": output("git", "-C", str(REPO), "rev-parse", "HEAD"),
        "dirty": bool(output("git", "-C", str(REPO), "status", "--porcelain")),
    }
    try:
        run("docker", "run", "--detach", "--rm", "--name", name,
            "--cpus=0.25", "--memory=768m", "--memory-swap=768m", "--pids-limit=128",
            "-p", f"127.0.0.1:{rustfs_port}:9000",
            "-e", "RUSTFS_ACCESS_KEY=" + storage["access_key"],
            "-e", "RUSTFS_SECRET_KEY=" + storage["secret_key"], RUSTFS_IMAGE, "/data",
            stdout=subprocess.DEVNULL)
        wait_port(rustfs_port)
        # Bucket setup finishes before the measured server and client start.
        for attempt in range(20):
            result = subprocess.run([
                "docker", "run", "--rm", "--name", rc_name, "--network", "container:" + name,
                "--cpus=0.25", "--memory=128m", "--memory-swap=128m", "--entrypoint", "/bin/sh",
                RC_IMAGE, "-c", "rc alias set probe http://127.0.0.1:9000 "
                f"{storage['access_key']} {storage['secret_key']} && "
                f"rc mb --ignore-existing probe/{storage['bucket']}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, timeout=30)
            if result.returncode == 0:
                break
            if attempt == 19:
                raise RuntimeError("RustFS bucket setup failed: " + result.stderr)
            time.sleep(0.25)
        rustfs_pid = output("docker", "inspect", "--format", "{{.State.Pid}}", name)
        temporary = directory / "tmp"
        temporary.mkdir()
        environment = {**os.environ, "TOKIO_WORKER_THREADS": "2", "TMPDIR": str(temporary),
                       "AUCTIONMARK_ENVIRONMENT": json.dumps(metadata)}
        with (directory / "server.log").open("w") as log:
            server = subprocess.Popen([str(binary), str(config_path)], stdout=log, stderr=log,
                                      env=environment, start_new_session=True)
            wait_port(port, server)
            command = ["systemd-run", "--user", "--scope", "--quiet",
                       "-p", "CPUQuota=200%", "-p", "MemoryMax=2G",
                       "-p", "MemorySwapMax=0", "-p", "TasksMax=1024",
                       "nice", "-n", "10",
                       "clojure", "-J-Xmx768m", "-J-XX:ActiveProcessorCount=2", "-M:sync",
                       *benchmark_args, "--host", "127.0.0.1", "--port", str(port),
                       "--server-pid", str(server.pid), "--storage-pid", rustfs_pid,
                       "--memory-mib", str(metadata["combined_memory_mib"]),
                       "--output", str(directory / "report.json")]
            client = subprocess.Popen(command, cwd=BENCH, env=environment, start_new_session=True)
            result = client.wait(timeout=args.timeout)
            metadata["exit_code"] = result
            return result
    except Exception as error:
        metadata["error"] = str(error)
        raise
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, signal.SIG_IGN)
        stop(client)
        stop(server)
        try:
            with (directory / "rustfs.log").open("w") as log:
                subprocess.run(["docker", "logs", name], stdout=log, stderr=subprocess.STDOUT, timeout=15)
        finally:
            try:
                subprocess.run(["docker", "rm", "--force", rc_name, name], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=30)
            finally:
                (directory / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
                print("Artifacts:", directory, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-binary", default=str(Path(os.getenv("CARGO_TARGET_DIR", REPO / "target")) / "release" / "triplox"))
    parser.add_argument("--output-dir", default=str(REPO / "target" / "auctionmark-sync" / datetime.datetime.now().strftime("%Y%m%d-%H%M%S")))
    parser.add_argument("--timeout", type=int, default=600, help="Client timeout in seconds")
    args, remaining = parser.parse_known_args()
    if remaining[:1] == ["--"]:
        remaining = remaining[1:]
    return benchmark(args, remaining)


if __name__ == "__main__":
    sys.exit(main())
