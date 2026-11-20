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
            headers["RateLimit-Policy"] = '"resolvers";q=5000;w=300'
        else:
            self.send_error(404)
            return
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
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

    def test_server_ignores_range_and_sends_full_file(self):
        part = self.seed_part()
        self.server.ignore_range = True
        result = self.cli()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(part.exists())
        self.assertEqual((self.dest / "data/test.bin").read_bytes(), FILES["data/test.bin"])
