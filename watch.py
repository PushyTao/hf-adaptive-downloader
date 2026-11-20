"""Supervise one dataset download and resume it after temporary failures."""
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from contextlib import closing

from download import parse_args
from hub import log
from storage import destination, Layout, meta, single_run, STATE


def launch(command, run_id):
    options = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {"start_new_session": True}
    return subprocess.Popen(command, env=dict(os.environ, HF_ADAPTIVE_RUN_ID=run_id), **options)


def stop_child(child):
    if child.poll() is not None:
        return
    try:
        child.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT)
        child.wait(timeout=55)
    except (OSError, subprocess.TimeoutExpired, KeyboardInterrupt):
        if child.poll() is None:
            child.kill()
        child.wait()


def retry_delay(code, consecutive, base):
    if code == 75:
        return min(900, base * 2 ** min(5, consecutive))
    crash = code == 1 or code < 0 or code >= 0x80000000
    if (crash or code == 76) and consecutive < 2:
        return base * 2 ** consecutive
    return None


def check_report(layout, repo, requested, run_id):
    report = json.loads(layout.path(STATE + "/verification.json").read_text(encoding="utf-8"))
    manifest = layout.path(STATE + "/manifest.sqlite")
    if not manifest.is_file():
        raise ValueError("Missing snapshot manifest")
    with closing(sqlite3.connect(str(manifest))) as db:
        count, total = db.execute("SELECT count(*),coalesce(sum(size),0) FROM files").fetchone()
        rev = meta(db, "revision")
        valid = (meta(db, "complete") == "1" and meta(db, "repo") == repo and
                 requested in (meta(db, "ref"), rev) and report.get("revision") == rev)
    if not (valid and report.get("run_id") == run_id and report.get("repository") == repo and
            report.get("complete") is True and report.get("verified") == report.get("files") == count and
            report.get("bytes") == total and report.get("missing_or_corrupt") == 0):
        raise ValueError("Verification report does not match this run and manifest")


def supervise(args, argv, base_delay=30):
    layout = Layout(destination(args.destination))
    if args.verify_only and not layout.path(STATE + "/manifest.sqlite").is_file():
        raise ValueError("Saved manifest required")
    layout.state.mkdir(parents=True, exist_ok=True)
    identity = layout.root.stat()
    command = [sys.executable, "-u", str(Path(__file__).with_name("download.py")), *argv]
    consecutive = 0
    previous = None

    def check_disk():
        if not os.path.samestat(identity, layout.root.stat()):
            raise OSError("Destination changed; reconnect the original disk")

    with single_run(layout.path(STATE + "/watch.lock")):
        while True:
            check_disk()
            run_id = uuid.uuid4().hex
            started = time.monotonic()
            child = launch(command, run_id)
            try:
                log("Watchdog: started PID %d for %s" % (child.pid, args.repo))
                code = child.wait()
            finally:
                stop_child(child)
            check_disk()
            if code == 0:
                check_report(layout, args.repo, args.revision, run_id)
                log("Watchdog: verified all files")
                return 0
            if code in (130, -signal.SIGINT, 0xC000013A, -1073741510):
                return 130
            if previous != code or time.monotonic() - started >= 3600:
                consecutive = 0
            delay = retry_delay(code, consecutive, base_delay)
            if delay is None or (args.verify_only and code == 76):
                log("Watchdog: stopped with exit code %d" % code)
                return code if 0 < code < 256 else 1
            previous = code
            consecutive += 1
            log("Watchdog: exit %d, resuming in %ds" % (code, delay))
            time.sleep(delay)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    try:
        return supervise(parse_args(argv, description=__doc__), argv)
    except KeyboardInterrupt:
        log("Watchdog: stopped by user")
        return 130
    except (ValueError, OSError, sqlite3.Error) as exc:
        log("Watchdog: " + str(exc))
        return 2


if __name__ == "__main__":
    sys.exit(main())
