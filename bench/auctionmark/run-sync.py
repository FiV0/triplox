#!/usr/bin/env python3
"""Run a disposable MinIO-backed benchmark with a combined two-core/8 GiB budget."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import uuid

MINIO_IMAGE = "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
MC_IMAGE = "minio/mc:RELEASE.2025-08-13T08-35-41Z"
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
    mc_name = name + "-setup"
    server = client = None
    with binary.open("rb") as executable:
        digest = hashlib.file_digest(executable, "sha256").hexdigest()
    metadata = {
        "runtime": "pooled", "foreground_threads": 2, "merger_threads": 1,
        "shared_cache_mib": 4096, "cdc_poll_ms": 200,
        "remote_flush_us": 100, "storage": "MinIO", "log": "local file",
        "minio_image": MINIO_IMAGE, "server_binary": str(binary),
        "server_sha256": digest,
        "application_cpu_quota": 1.75, "application_memory_mib": 7424,
        "minio_cpu_quota": 0.25, "minio_memory_mib": 768,
        "revision": output("git", "-C", str(REPO), "rev-parse", "HEAD"),
        "dirty": bool(output("git", "-C", str(REPO), "status", "--porcelain")),
    }
    try:
        run("docker", "run", "--detach", "--rm", "--name", name,
            "--cpus=0.25", "--memory=768m", "--memory-swap=768m", "--pids-limit=128",
            "-p", "127.0.0.1::9000", "-e", "MINIO_ROOT_USER=triplox",
            "-e", "MINIO_ROOT_PASSWORD=triplox123", MINIO_IMAGE, "server", "/data",
            stdout=subprocess.DEVNULL)
        minio_port = int(output("docker", "port", name, "9000/tcp").rsplit(":", 1)[1])
        wait_port(minio_port)
        # Bucket setup finishes before the measured server and client start.
        for attempt in range(20):
            result = subprocess.run([
                "docker", "run", "--rm", "--name", mc_name, "--network", "container:" + name,
                "--cpus=0.25", "--memory=128m", "--memory-swap=128m",
                "-e", "MC_HOST_probe=http://triplox:triplox123@127.0.0.1:9000",
                MC_IMAGE, "mb", "--ignore-existing", "probe/auctionmark"],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, timeout=30)
            if result.returncode == 0:
                break
            if attempt == 19:
                raise RuntimeError("MinIO bucket setup failed: " + result.stderr)
            time.sleep(0.25)
        minio_pid = output("docker", "inspect", "--format", "{{.State.Pid}}", name)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        # JSON string quoting is compatible with these TOML basic strings.
        config = directory / "server.toml"
        config.write_text(
            f'[storage]\ntype="remote"\nendpoint="http://127.0.0.1:{minio_port}"\n'
            'bucket="auctionmark"\naccess_key="triplox"\nsecret_key="triplox123"\n'
            f'cache_path={json.dumps(str(directory / "cache"))}\n'
            'wal_flush_interval_us=100\n'
            f'[log]\ntype="file"\npath={json.dumps(str(directory / "log" / "log"))}\n'
            f'[server]\nhost="127.0.0.1"\nport={port}\n'
            '[incremental]\nruntime="pooled"\nthreads=2\nmerger_threads=1\ncache_mib=4096\n')
        temporary = directory / "tmp"
        temporary.mkdir()
        environment = {**os.environ, "TOKIO_WORKER_THREADS": "2", "TMPDIR": str(temporary),
                       "AUCTIONMARK_ENVIRONMENT": json.dumps(metadata)}
        with (directory / "server.log").open("w") as log:
            server = subprocess.Popen([str(binary), str(config)], stdout=log, stderr=log,
                                      env=environment, start_new_session=True)
            wait_port(port, server)
            command = ["clojure", "-J-Xmx768m", "-J-XX:ActiveProcessorCount=2", "-M:sync",
                       *benchmark_args, "--host", "127.0.0.1", "--port", str(port),
                       "--server-pid", str(server.pid), "--minio-pid", minio_pid,
                       "--memory-mib", "8192", "--output", str(directory / "report.json")]
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
            with (directory / "minio.log").open("w") as log:
                subprocess.run(["docker", "logs", name], stdout=log, stderr=subprocess.STDOUT, timeout=15)
        finally:
            try:
                subprocess.run(["docker", "rm", "--force", mc_name, name], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=30)
            finally:
                (directory / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
                print("Artifacts:", directory, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-binary", default=str(Path(os.getenv("CARGO_TARGET_DIR", REPO / "target")) / "release" / "triplox"))
    parser.add_argument("--output-dir", default=str(REPO / "target" / "auctionmark-sync" / datetime.datetime.now().strftime("%Y%m%d-%H%M%S")))
    parser.add_argument("--timeout", type=int, default=600, help="Client timeout in seconds")
    parser.add_argument("--inside-scope", action="store_true", help=argparse.SUPPRESS)
    args, remaining = parser.parse_known_args()
    if remaining[:1] == ["--"]:
        remaining = remaining[1:]
    if args.inside_scope:
        return benchmark(args, remaining)
    # Docker's daemon applies its own separate 0.25-core/768 MiB container limit.
    command = ["systemd-run", "--user", "--scope", "--quiet", "-p", "CPUQuota=175%",
               "-p", "MemoryMax=7424M", "-p", "MemorySwapMax=0", "-p", "TasksMax=1024",
               "nice", "-n", "10", sys.executable, str(Path(__file__).resolve()),
               "--inside-scope", "--server-binary", args.server_binary,
               "--output-dir", args.output_dir, "--timeout", str(args.timeout), "--", *remaining]
    return subprocess.call(command)


if __name__ == "__main__":
    sys.exit(main())
