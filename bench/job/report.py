"""Regenerate reports and compare saved JOB answers without running databases."""

import csv
import hashlib
import json
import math
from pathlib import Path


def read_json(path, default=None):
    return json.loads(path.read_text()) if path.exists() else default


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def report(root):
    root = Path(root)
    manifest = read_json(root / "manifest.json")
    if manifest is None:
        raise ValueError("Missing run manifest")
    summaries, queries, answers, mismatches, problems = [], [], {}, [], []
    configurations = []
    for directory in sorted(root.iterdir()):
        config = read_json(directory / "config.json") if directory.is_dir() else None
        if config is None:
            continue
        configurations.append({k: config.get(k) for k in ["data-dir", "workload", "queries", "batch-size", "max-bytes", "cycles", "seed", "ingest-only"]})
        events_file = directory / "measurements.jsonl"
        events = [json.loads(line) for line in events_file.read_text().splitlines()] if events_file.exists() else []
        samples_file = directory / "resources.jsonl"
        resources = [json.loads(line) for line in samples_file.read_text().splitlines()] if samples_file.exists() else []
        refreshes = [e for e in events if e["phase"] == "refresh" and e["status"] == "ok"]
        first = next((e for e in events if e["phase"] == "initial"), {})
        initial_queries = next((e for e in events if e["phase"] == "query-pass" and e["checkpoint"] == "initial"), {})
        ingestion = next((e for e in events if e["phase"] == "ingestion"), {})
        ingest_only = config.get("ingest-only", False)
        warmed = any(e["phase"] == "warmup" and e["status"] == "ok" for e in events)
        durations = [e["elapsed-ms"] for e in refreshes]
        queries.extend(dict(e, engine=directory.name) for e in events if e["phase"] == "query")
        summaries.append({"engine": directory.name,
                          "status": read_json(directory / "status.json", {"status": "incomplete"})["status"],
                          "cache-state": "warmed" if warmed else "not explicitly warmed",
                          "initial-ms": first.get("elapsed-ms", initial_queries.get("elapsed-ms")),
                          "initial-metric": "infrastructure check only" if ingest_only else ("ingestion + catch-up" if first else "query pass on loaded data"),
                          "ingestion-ms": ingestion.get("elapsed-ms", first.get("ingestion-ms")), "catchup-ms": first.get("catchup-ms"),
                          "refresh-count": len(refreshes), "refresh-p50-ms": percentile(durations, .5),
                          "refresh-p95-ms": percentile(durations, .95),
                          "source-rows-per-second": sum(e["source-rows"] for e in refreshes) * 1000 / sum(durations) if sum(durations) else None,
                          "runner-peak-rss-bytes": max((s.get("runner-rss-bytes", 0) for s in resources), default=0),
                          "transactor-peak-rss-bytes": max((s.get("transactor-rss-bytes", 0) for s in resources), default=0)})
        answers[directory.name] = {str(p.relative_to(directory / "answers")): hashlib.sha256(p.read_bytes()).hexdigest()
                                   for p in (directory / "answers").rglob("*.edn")}
        if not ingest_only:
            selected = read_json(directory / "selection.json", [])
            if not selected:
                problems.append(f"{directory.name}: missing query selection")
            trace = read_json(directory / "trace.json", {})
            checkpoints = ["initial"] + [f"batch-{n}" for n in range(trace.get("batches", 0))]
            required = {f"{checkpoint}/{query}.edn" for checkpoint in checkpoints for query in selected}
            if set(answers[directory.name]) != required:
                problems.append(f"{directory.name}: missing or unexpected checkpoint answers")
            if config["workload"] == "maintenance" and (not trace or len(refreshes) != trace.get("batches")):
                problems.append(f"{directory.name}: incomplete maintenance trace")
            if directory.name in ("datalevin", "datomic") and not warmed:
                problems.append(f"{directory.name}: warmup did not complete")
            if not first and not initial_queries:
                problems.append(f"{directory.name}: initial measurement missing")
    if not summaries:
        problems.append("No engine artifacts")
    if configurations and any(c != configurations[0] for c in configurations):
        raise ValueError("Refusing to compare incompatible workload configurations")
    trace_hashes = [read_json(root / engine / "trace.json") for engine in answers]
    available_traces = [trace for trace in trace_hashes if trace]
    if available_traces and any(t != available_traces[0] for t in available_traces):
        raise ValueError("Maintenance trace hashes differ")
    if len(answers) > 1:
        keys = set().union(*(set(value) for value in answers.values()))
        for key in sorted(keys):
            hashes = {engine: value.get(key) for engine, value in answers.items()}
            if None in hashes.values() or len(set(hashes.values())) != 1:
                mismatches.append({"answer": key, "hashes": hashes})
    compared = len(answers) > 1 and any(answers.values())
    (root / "comparison.json").write_text(json.dumps({"mismatches": mismatches, "problems": problems,
                                                       "cross-engine-verified": compared and not mismatches and not problems and all(row["status"] == "ok" for row in summaries),
                                                       "engines": list(answers)}, indent=2) + "\n")
    for filename, rows in [("summary.csv", summaries), ("queries.csv", queries)]:
        with (root / filename).open("w") as stream:
            columns = sorted(set().union(*(row.keys() for row in rows)))
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
    lines = ["# JOB results", "", f"Run: `{manifest['run-id']}`", "",
             "Datalevin and Datomic query timings exclude loading and the complete cache-warmup pass.",
             "Triplox incremental initial timings include ingestion and client-view catch-up.", "",
             "| Engine | Status | Cache state | Initial metric | Milliseconds | Refresh p95 ms |",
             "|---|---|---|---|---:|---:|"]
    for row in summaries:
        lines.append(f"| {row['engine']} | {row['status']} | {row['cache-state']} | {row['initial-metric']} | {row['initial-ms']} | {row['refresh-p95-ms']} |")
    lines += ["", f"Cross-engine answer mismatches or missing answers: {len(mismatches)}.",
              "" if compared else "Cross-engine verification was not performed: fewer than two engines supplied answers.",
              *problems,
              "", "Failed and incomplete runs are not full-suite performance results."]
    (root / "summary.md").write_text("\n".join(lines) + "\n")
    print(f"Report: {root / 'summary.md'}")
    return not mismatches and not problems and all(row["status"] == "ok" for row in summaries)
