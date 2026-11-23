#!/usr/bin/env python3
"""Run a disposable RustFS-backed benchmark with separate server and client limits."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import string
import subprocess
import sys
import threading
import time
import tomllib
import uuid

RUSTFS_IMAGE = "rustfs/rustfs:1.0.0"
RC_IMAGE = "rustfs/rc:v0.1.36"
BENCH = Path(__file__).resolve().parent
REPO = BENCH.parent.parent
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
TICKS = os.sysconf("SC_CLK_TCK")
UNITS = {"B": 1, "kB": 10**3, "KiB": 2**10, "MB": 10**6, "MiB": 2**20, "GB": 10**9, "GiB": 2**30}


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


def process_tree(root):
    """root and all its descendants, from the parent PIDs in /proc."""
    children = {}
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                ppid = int((entry / "stat").read_text().rsplit(")", 1)[1].split()[1])
            except (OSError, IndexError, ValueError):
                continue
            children.setdefault(ppid, []).append(int(entry.name))
    pids, pending = [], [root]
    while pending:
        pid = pending.pop()
        pids.append(pid)
        pending.extend(children.get(pid, []))
    return pids


def tree_sample(root):
    """Summed RSS, CPU time, threads and open files of a process tree, or None after it exits."""
    if not Path(f"/proc/{root}").exists():
        return None
    sample = {"rss-bytes": 0, "cpu-seconds": 0.0, "threads": 0, "file-descriptors": 0}
    for pid in process_tree(root):
        try:
            # Fields after the command name, starting at field 3 (state).
            fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            descriptors = len(os.listdir(f"/proc/{pid}/fd"))
        except OSError:
            continue
        sample["cpu-seconds"] += (int(fields[11]) + int(fields[12])) / TICKS
        sample["threads"] += int(fields[17])
        sample["rss-bytes"] += int(fields[21]) * PAGE_SIZE
        sample["file-descriptors"] += descriptors
    return sample


def container_sample(name):
    try:
        stats = run("docker", "stats", "--no-stream", "--format", "{{json .}}", name,
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout
        return {"rustfs": json.loads(stats)}
    except (subprocess.SubprocessError, ValueError):
        return {"error": "docker stats failed"}


def record(path, take, interval, stop):
    """Append a timestamped sample to path every interval seconds until stop is set."""
    with path.open("w") as out:
        while not stop.is_set():
            out.write(json.dumps({"at": time.time(), **take()}) + "\n")
            out.flush()
            stop.wait(interval)


def size_bytes(text):
    number, unit = re.fullmatch(r"([\d.]+)\s*([A-Za-z]+)", text.strip()).groups()
    return float(number) * UNITS[unit]


def summarize(directory):
    """Peaks and average CPU per process tree and for the RustFS container."""
    def samples(name):
        path = directory / name
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    processes, containers, summary = samples("processes.jsonl"), samples("containers.jsonl"), {}
    for role in ("server", "client"):
        seen = [(s["at"], s["processes"][role]) for s in processes if s["processes"][role]]
        if seen:
            (start, first), (end, last) = seen[0], seen[-1]
            summary[role] = {
                "peak-rss-mib": max(p["rss-bytes"] for _, p in seen) / 2**20,
                "peak-threads": max(p["threads"] for _, p in seen),
                "peak-file-descriptors": max(p["file-descriptors"] for _, p in seen),
                "average-cpu-cores": (last["cpu-seconds"] - first["cpu-seconds"]) / (end - start) if end > start else None}
    rustfs = [s["rustfs"] for s in containers if "rustfs" in s]
    if rustfs:
        summary["rustfs"] = {
            "peak-memory-mib": max(size_bytes(s["MemUsage"].split("/")[0]) for s in rustfs) / 2**20,
            "average-cpu-percent": sum(float(s["CPUPerc"].rstrip("%")) for s in rustfs) / len(rustfs)}
    # Server and client are sampled together; the RustFS peak is added as an upper bound.
    summary["combined-peak-mib"] = (
        max((sum(p["rss-bytes"] for p in s["processes"].values() if p) for s in processes), default=0) / 2**20
        + summary.get("rustfs", {}).get("peak-memory-mib", 0))
    return summary


def add_resources(directory, limit_mib, exit_code):
    """Add the resource summary to the client's report and fail the run above limit_mib."""
    summary = summarize(directory)
    (directory / "resources.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Combined peak memory {summary['combined-peak-mib']:.0f} MiB (limit {limit_mib} MiB)", flush=True)
    over = summary["combined-peak-mib"] > limit_mib
    report = directory / "report.json"
    if report.exists():
        data = json.loads(report.read_text())
        data["resources"] = summary
        if over:
            data["passed?"] = False
            data["failed-checks"] = data.get("failed-checks", []) + ["memory"]
        report.write_text(json.dumps(data))
    return 1 if over else exit_code


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
                       "--output", str(directory / "report.json")]
            client = subprocess.Popen(command, cwd=BENCH, env=environment, start_new_session=True)
            # Sampling runs here, outside the measured processes. docker stats takes about a second.
            sampling = threading.Event()
            samplers = [
                threading.Thread(target=record, daemon=True, args=(
                    directory / "processes.jsonl",
                    lambda: {"processes": {"server": tree_sample(server.pid), "client": tree_sample(client.pid)}},
                    0.5, sampling)),
                threading.Thread(target=record, daemon=True, args=(
                    directory / "containers.jsonl", lambda: container_sample(name), 1, sampling))]
            for sampler in samplers:
                sampler.start()
            try:
                result = client.wait(timeout=args.timeout)
            finally:
                sampling.set()
                for sampler in samplers:
                    sampler.join(timeout=10)
            result = add_resources(directory, metadata["combined_memory_mib"], result)
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
