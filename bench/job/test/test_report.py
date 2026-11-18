import json
from pathlib import Path
import tempfile
import unittest

from report import report


class ReportTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.write(self.root / "manifest.json", {"run-id": "test", "format-version": 4})

    def write(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def engine(self, engine, answer='[["answer"]]', status="ok", ingest_only=False):
        directory = self.root / engine
        self.write(directory / "config.json", {"queries": "1a", "ingest-only": ingest_only})
        self.write(directory / "status.json", {"status": status})
        self.write(directory / "selection.json", ["1a"])
        if not ingest_only:
            answer_file = directory / "answers" / "1a.edn"
            answer_file.parent.mkdir(parents=True)
            answer_file.write_text(answer)
            events = [{"phase": "warmup", "status": "ok"},
                      {"phase": "query-pass", "status": "ok", "elapsed-ms": 12}]
            if engine == "triplox-incremental":
                events = [{"phase": "initial", "status": "ok", "elapsed-ms": 12},
                          {"phase": "verification", "status": "ok", "queries": 1}]
            (directory / "measurements.jsonl").write_text("\n".join(map(json.dumps, events)))
        return directory

    def test_matching_answers(self):
        self.engine("datalevin")
        self.engine("triplox-standard")
        self.assertTrue(report(self.root))
        self.assertTrue(json.loads((self.root / "comparison.json").read_text())["cross-engine-verified"])

    def test_summary_shows_ingestion_time(self):
        directory = self.engine("datalevin")
        with (directory / "measurements.jsonl").open("a") as stream:
            stream.write("\n" + json.dumps({"phase": "ingestion", "status": "ok", "elapsed-ms": 34}))
        self.engine("triplox-standard")
        self.assertTrue(report(self.root))
        summary = (self.root / "summary.md").read_text()
        self.assertIn("| datalevin | ok | warmed | query pass on loaded data | 12 | 34 |", summary)
        self.assertIn("| triplox-standard | ok | warmed | query pass on loaded data | 12 | n/a |", summary)

    def test_mismatch_and_missing_answers_fail(self):
        self.engine("datalevin")
        directory = self.engine("triplox-standard", "[]")
        self.assertFalse(report(self.root))
        (directory / "answers/1a.edn").unlink()
        self.assertFalse(report(self.root))

    def test_failed_runs_and_missing_warmup_fail(self):
        directory = self.engine("datalevin", status="failed")
        self.assertFalse(report(self.root))
        self.write(directory / "status.json", {"status": "ok"})
        (directory / "measurements.jsonl").write_text("")
        self.assertFalse(report(self.root))

    def test_incompatible_workloads_are_rejected(self):
        self.engine("datalevin")
        directory = self.engine("triplox-standard")
        self.write(directory / "config.json", {"queries": "2a"})
        with self.assertRaises(ValueError):
            report(self.root)

    def test_no_answers_are_not_verification(self):
        self.assertFalse(report(self.root))
        self.engine("datalevin", ingest_only=True)
        self.engine("triplox-standard", ingest_only=True)
        self.assertTrue(report(self.root))
        self.assertFalse(json.loads((self.root / "comparison.json").read_text())["cross-engine-verified"])

    def test_final_answers_match_across_standard_and_incremental_runs(self):
        self.engine("triplox-standard")
        self.engine("triplox-incremental")
        self.assertTrue(report(self.root))
        comparison = json.loads((self.root / "comparison.json").read_text())
        self.assertTrue(comparison["cross-engine-verified"])
        self.assertTrue(comparison["incremental-standard-verified"])

    def test_standalone_incremental_run_requires_standard_query_verification(self):
        directory = self.engine("triplox-incremental")
        self.assertTrue(report(self.root))
        comparison = json.loads((self.root / "comparison.json").read_text())
        self.assertFalse(comparison["cross-engine-verified"])
        self.assertTrue(comparison["incremental-standard-verified"])
        events_file = directory / "measurements.jsonl"
        events = [json.loads(line) for line in events_file.read_text().splitlines()]
        events_file.write_text("\n".join(json.dumps(e) for e in events
                                       if e["phase"] != "verification"))
        self.assertFalse(report(self.root))
        self.assertFalse(json.loads((self.root / "comparison.json").read_text())["incremental-standard-verified"])

    def test_older_run_formats_are_rejected(self):
        self.engine("datalevin")
        self.write(self.root / "manifest.json", {"run-id": "test", "format-version": 3})
        with self.assertRaises(ValueError):
            report(self.root)
