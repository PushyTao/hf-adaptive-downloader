import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
REV = "a" * 40
FILES = {"notes.txt": b"hello\n", "data/test.bin": b"0123456789" * 600}


class Hub(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        path = urlsplit(self.path).path
        headers = {}
        status = 200
        if "/revision/" in path:
            body = json.dumps({"sha": REV}).encode()
        elif "/tree/" in path:
            rows = []
            for name, data in FILES.items():
                sha1 = hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()
                row = dict(type="file", path=name, size=len(data), oid=sha1)
                if name.endswith(".bin"):
                    row["lfs"] = {"oid": hashlib.sha256(data).hexdigest()}
                rows.append(row)
            if "page=2" in self.path:
                rows = rows[1:]
            else:
                headers["Link"] = '<%s?page=2>; rel="next"' % path
                rows = rows[:1]
            body = json.dumps(rows).encode()
        elif "/resolve/" in path:
            name = path.split("/resolve/" + REV + "/")[1]
            offset = self.headers.get("Range")
            self.server.resolves.append((name, offset))
            body = FILES[name]
            if offset and not self.server.ignore_range:
                start = int(offset[6:-1])
                headers["Content-Range"] = "bytes %d-%d/%d" % (start, len(body)-1, len(body))
                body = body[start:]
                status = 206
            headers["Accept-Ranges"] = "bytes"
            headers["RateLimit-Policy"] = '"resolvers";q=5000;w=300'
        else:
            self.send_error(404)
            return
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        delay = getattr(self.server, "hold_first_body", None)
        if delay and path.endswith("test.bin") and not self.headers.get("Range"):
            self.wfile.write(body[:2*1024**2])
            self.wfile.flush()
            delay.wait(15)
            try:
                self.wfile.write(body[2*1024**2:])
            except OSError:
                pass
        else:
            self.wfile.write(body)


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.dest = Path(temp.name) / "snapshot"
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Hub)
        self.server.resolves = []
        self.server.ignore_range = False
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.endpoint = "http://127.0.0.1:%d" % self.server.server_port

    def cli(self, *args, script="download.py", repo="me/sample"):
        return subprocess.run(
            [sys.executable, str(ROOT / script), repo, str(self.dest),
             "--endpoint", self.endpoint, *args],
            capture_output=True, text=True, timeout=20)

    def seed_part(self):
        key = hashlib.sha256(b"data/test.bin").hexdigest()
        part = self.dest / ".hf-adaptive" / "parts" / key[:2] / key
        part.parent.mkdir(parents=True)
        part.write_bytes(FILES["data/test.bin"][:100])
        return part

    def test_resume_verify_offline_and_repair_only_corrupt_file(self):
        self.seed_part()
        self.dest.joinpath("notes.txt").write_bytes(FILES["notes.txt"])
        first = self.cli()
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        self.assertEqual(self.server.resolves, [("data/test.bin", "bytes=100-")])
        self.assertEqual((self.dest / "data/test.bin").read_bytes(), FILES["data/test.bin"])
        self.dest.joinpath("notes.txt").write_bytes(b"wrong!")
        self.assertEqual(self.cli("--verify-only").returncode, 76)
        repair = self.cli()
        self.assertEqual(repair.returncode, 0, repair.stdout)
        self.assertEqual(self.server.resolves[-1], ("notes.txt", None))
        report = json.loads((self.dest / ".hf-adaptive/verification.json").read_text())
        self.assertEqual((report["revision"], report["verified"], report["complete"]), (REV, 2, True))
        wrong = self.cli(repo="other/data")
        self.assertEqual(wrong.returncode, 2)
        self.assertEqual(len(self.server.resolves), 2)

    def test_yielded_transfer_resumes_without_counting_bytes_twice(self):
        from unittest.mock import patch
        from hub import Hub
        from storage import Layout
        from transfer import Traffic, fetch
        data = FILES["data/test.bin"]
        row = ("data/test.bin", "data/test.bin", len(data), "sha256", hashlib.sha256(data).hexdigest())
        hub = Hub(self.endpoint, "me/sample", threading.Event())
        layout = Layout(self.dest)
        traffic = Traffic()
        yield_slot = threading.Event()
        yield_slot.set()
        with patch("transfer.BLOCK", 1024):
            self.assertFalse(fetch(hub, REV, layout, row, traffic, yield_slot))
            self.assertFalse((self.dest / row[1]).exists())
            yield_slot.clear()
            self.assertTrue(fetch(hub, REV, layout, row, traffic, yield_slot))
        self.assertEqual(self.server.resolves, [(row[0], None), (row[0], "bytes=1024-")])
        self.assertEqual(traffic.snapshot(), (len(data), len(data)))

    def test_watchdog_restarts_killed_child_and_resumes_partial(self):
        from unittest.mock import patch
        import time
        import watch
        from download import parse_args

        self.server.hold_first_body = threading.Event()
        self.addCleanup(self.server.hold_first_body.set)
        payload = b"a" * (4*1024**2)
        key = hashlib.sha256(b"data/test.bin").hexdigest()
        part = self.dest / ".hf-adaptive/parts" / key[:2] / key
        argv = ["me/sample", str(self.dest), "--endpoint", self.endpoint]
        children = []
        real_launch = watch.launch

        def launch_and_interrupt(command, run_id):
            child = real_launch(command, run_id)
            children.append(child)
            if len(children) == 1:
                until = time.monotonic() + 10
                while time.monotonic() < until:
                    if part.exists() and part.stat().st_size >= 1024**2:
                        break
                    time.sleep(.02)
                child.kill()
                child.wait()
                self.server.hold_first_body.set()
            return child

        with patch.dict(FILES, {"data/test.bin": payload}), patch.object(watch, "launch", launch_and_interrupt):
            self.assertEqual(watch.supervise(parse_args(argv), argv, 1), 0)
        self.assertEqual(len(children), 2)
        self.assertTrue(any(name.endswith(".bin") and r for name, r in self.server.resolves))
        self.assertEqual((self.dest / "data/test.bin").read_bytes(), payload)

    def test_watchdog_cli_checks_report(self):
        result = self.cli("--workers", "2", script="watch.py")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Watchdog: verified", result.stdout)
        from watch import check_report
        from storage import Layout
        with self.assertRaises(ValueError):
            check_report(Layout(self.dest), "me/sample", "main", "unrelated-run")

    def test_import_legacy_part_requires_same_snapshot_and_resumes_by_range(self):
        import sqlite3
        repo = "sdu-chenxin/MDS-Bench-data"
        key = hashlib.sha256(b"data/test.bin").hexdigest()
        old = self.dest / ".mds-state"
        part = old / "parts" / key[:2] / key
        part.parent.mkdir(parents=True)
        part.write_bytes(FILES["data/test.bin"][:321])
        (old / "run.lock").write_bytes(b"0")
        db = sqlite3.connect(str(old / "manifest.sqlite"))
        self.addCleanup(db.close)
        db.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, val TEXT)")
        db.executemany("INSERT INTO meta VALUES (?,?)", [("complete", "1"), ("revision", "b"*40)])
        db.commit()
        mismatch = self.cli("--revision", REV, "--reuse-mds-parts", repo=repo)
        self.assertEqual(mismatch.returncode, 2)
        self.assertEqual(self.server.resolves, [])
        self.assertEqual(part.stat().st_size, 321)
        db.execute("UPDATE meta SET val=? WHERE key='revision'", (REV,))
        db.commit()
        result = self.cli("--revision", REV, "--reuse-mds-parts", repo=repo)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(("data/test.bin", "bytes=321-"), self.server.resolves)
        self.assertFalse(part.exists())
        self.assertEqual((self.dest / "data/test.bin").read_bytes(), FILES["data/test.bin"])

    def test_repair_rereads_only_files_that_failed_full_verification(self):
        from unittest.mock import patch
        import download
        from storage import digest_ok
        self.dest.mkdir()
        (self.dest / "data").mkdir()
        (self.dest / "notes.txt").write_bytes(b"broken")
        (self.dest / "data/test.bin").write_bytes(FILES["data/test.bin"])
        args = download.parse_args(["me/sample", str(self.dest), "--endpoint", self.endpoint])
        with patch.object(download, "digest_ok", wraps=digest_ok) as reader:
            download.run(args)
        checked = [call.args[1][0] for call in reader.call_args_list]
        self.assertEqual(checked.count("data/test.bin"), 1)
        self.assertEqual(checked.count("notes.txt"), 2)

    def test_server_ignores_range_and_sends_full_file(self):
        part = self.seed_part()
        self.server.ignore_range = True
        result = self.cli()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(part.exists())
        self.assertEqual((self.dest / "data/test.bin").read_bytes(), FILES["data/test.bin"])
