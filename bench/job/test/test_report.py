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
        self.write(self.root / "manifest.json", {"run-id": "test"})

    def write(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def engine(self, engine, answer='[["answer"]]', status="ok", ingest_only=False):
        directory = self.root / engine
        self.write(directory / "config.json", {"queries": "1a", "workload": "initial", "ingest-only": ingest_only})
        self.write(directory / "status.json", {"status": status})
        self.write(directory / "selection.json", ["1a"])
        if not ingest_only:
            answer_file = directory / "answers/initial/1a.edn"
            answer_file.parent.mkdir(parents=True)
            answer_file.write_text(answer)
            events = [{"phase": "warmup", "status": "ok"},
                      {"phase": "query-pass", "checkpoint": "initial", "status": "ok", "elapsed-ms": 12}]
            (directory / "measurements.jsonl").write_text("\n".join(map(json.dumps, events)))
        return directory

    def test_matching_answers(self):
        self.engine("datalevin")
        self.engine("datomic")
        self.assertTrue(report(self.root))
        self.assertTrue(json.loads((self.root / "comparison.json").read_text())["cross-engine-verified"])

    def test_mismatch_and_missing_answers_fail(self):
        self.engine("datalevin")
        directory = self.engine("datomic", "[]")
        self.assertFalse(report(self.root))
        (directory / "answers/initial/1a.edn").unlink()
        self.assertFalse(report(self.root))

    def test_failed_runs_and_missing_warmup_fail(self):
        directory = self.engine("datomic", status="failed")
        self.assertFalse(report(self.root))
        self.write(directory / "status.json", {"status": "ok"})
        (directory / "measurements.jsonl").write_text("")
        self.assertFalse(report(self.root))

    def test_incompatible_workloads_are_rejected(self):
        self.engine("datalevin")
        directory = self.engine("datomic")
        self.write(directory / "config.json", {"queries": "2a", "workload": "initial"})
        with self.assertRaises(ValueError):
            report(self.root)

    def test_no_answers_are_not_verification(self):
        self.assertFalse(report(self.root))
        self.engine("datalevin", ingest_only=True)
        self.engine("datomic", ingest_only=True)
        self.assertTrue(report(self.root))
        self.assertFalse(json.loads((self.root / "comparison.json").read_text())["cross-engine-verified"])
