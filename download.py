"""Download and verify a pinned Hugging Face dataset snapshot."""
import argparse
from typing import NamedTuple
import concurrent.futures as cf
from contextlib import closing, ExitStack
import json
import os
import shutil
import signal
import sqlite3
import sys
import threading
import time

import requests

from hub import Hub, IncompleteDownload, NETWORK_ERRORS, OFFICIAL, TemporaryHubError, log
from storage import (allocation_unit, COLUMNS, destination, digest_ok, file_size, Layout,
                     manifest, single_run, STATE, write_report)
from transfer import fetch, Traffic
from scheduler import AdaptiveMix, Pending, RecentSpeed


def parser(description):
    p = argparse.ArgumentParser(description=description)
    p.add_argument("repo", help="dataset ID, e.g. sdu-chenxin/MDS-Bench-data")
    p.add_argument("destination", help="local snapshot directory")
    p.add_argument("--revision", default="main", help="branch, tag, or full commit SHA")
    p.add_argument("--endpoint", default=os.getenv("HF_ENDPOINT") or OFFICIAL)
    p.add_argument("--workers", type=int, default=64, metavar="N")
    p.add_argument("--large-workers", type=int, metavar="N", help="initial largest-first slots")
    p.add_argument("--reuse-mds-parts", action="store_true", help="resume partials from a matching legacy MDS snapshot")
    p.add_argument("--fixed-mix", action="store_true", help="keep the initial split throughout the run")
    p.add_argument("--window", type=int, default=300, metavar="SECONDS", help="stable measurement window")
    p.add_argument("--verify-only", action="store_true", help="verify all files using the saved manifest offline")
    return p


def parse_args(argv=None, description=__doc__):
    p = parser(description)
    args = p.parse_args(argv)
    if not 1 <= args.workers <= 256:
        p.error("--workers must be in 1..256")
    if args.large_workers is None:
        args.large_workers = max(1, args.workers // 4)
    if not 0 <= args.large_workers <= args.workers:
        p.error("--large-workers must be in 0..workers")
    if args.window < 1:
        p.error("--window must be positive")
    if args.workers > 1 and not args.fixed_mix and not 0 < args.large_workers < args.workers:
        p.error("adaptive scheduling requires both lanes; use --fixed-mix for a single lane")
    return args


def build_queue(db, layout, files, total):
    unit = allocation_unit(layout.root)
    def allocated(n):
        return ((n + unit - 1) // unit) * unit
    needed = parts = space = 0
    last = time.monotonic()
    for index, (key, remote, name, size) in enumerate(db.execute("SELECT rowid,path,local,size FROM files"), 1):
        if file_size(layout.path(name)) != size:
            partial = max(0, min(size, file_size(layout.part(remote))))
            needed += size
            parts += partial
            space += allocated(size) - allocated(partial)
            db.execute("INSERT INTO jobs VALUES (?,?)", (key, size))
        if time.monotonic() - last >= 15:
            log("Scan %d/%d files" % (index, files))
            last = time.monotonic()
    pending = db.execute("SELECT count(*) FROM jobs").fetchone()[0]
    free = shutil.disk_usage(layout.root).free
    log("Queue %d files | existing %d | partial %.3f GB | needed %.3f GB | free %.3f GB" %
        (pending, files-pending, parts/1e9, space/1e9, free/1e9))
    if space > free:
        raise ValueError("Insufficient disk space")
    return total - needed + parts


class Slot(NamedTuple):
    rank: int
    large: bool
    size: int
    yield_slot: threading.Event


def transfer_queue(db, hub, revision, layout, args, files, total, baseline, check_drive):
    queue = Pending(key for key, in db.execute("SELECT id FROM jobs ORDER BY size,id"))
    if not queue:
        return
    traffic = Traffic()
    active = {}
    finished = files - len(queue)
    small_finished = 0
    mix = AdaptiveMix(args.workers, args.large_workers, args.window)
    last = time.monotonic()
    speed = RecentSpeed(last, 0, 0)
    log("Workers %d | initial L/S %d/%d | %s | %s" %
        (args.workers, mix.target, args.workers-mix.target,
         "fixed" if args.fixed_mix else "adaptive %ds" % args.window, hub.pacer.status()))
    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        def fill():
            large_count = sum(slot.large for slot in active.values())
            while queue and len(active) < args.workers:
                large = large_count < mix.target
                rank, key = queue.take(large)
                row = db.execute("SELECT " + COLUMNS + " FROM files WHERE rowid=?", (key,)).fetchone()
                slot = Slot(rank, large, row[2], threading.Event())
                future = pool.submit(fetch, hub, revision, layout, row, traffic, slot.yield_slot)
                active[future] = slot
                large_count += large
            return large_count

        try:
            large_count = fill()
            while active:
                ready, _ = cf.wait(active, timeout=1, return_when=cf.FIRST_COMPLETED)
                for future in ready:
                    slot = active.pop(future)
                    if future.result():
                        finished += 1
                        small_finished += slot.size < 1024**2
                    else:
                        queue.put_back(slot.rank)
                large_count = fill()
                now = time.monotonic()
                received, retained = traffic.snapshot()
                old_target = mix.target
                if not args.fixed_mix:
                    decision = mix.update(now, retained, small_finished, hub.signals(), large_count,
                                          len(active) == args.workers and bool(queue))
                    if decision:
                        log("Adaptive: %s | target L/S %d/%d" % (decision, mix.target, args.workers-mix.target))
                if old_target != mix.target:
                    # Release only surplus lanes. Ranged transfers yield after a persisted block.
                    surplus_large = large_count > mix.target
                    surplus = abs(large_count - mix.target)
                    for slot in active.values():
                        if slot.large == surplus_large and surplus:
                            slot.yield_slot.set()
                            surplus -= 1
                        else:
                            slot.yield_slot.clear()
                if now - last >= 15 or not active:
                    check_drive()
                    saved = min(total, max(0, baseline + retained))
                    raw, useful, eta = speed.read(now, received, retained, remaining=total-saved)
                    lhs = sum(slot.large for slot in active.values())
                    avg = sum(slot.size for slot in active.values()) / max(1, len(active))
                    log("Download %.2f%% | %d/%d files | %.3f/%.3f GB | %.2f MB/s received | "
                        "%.2f MB/s useful | ETA %s | %s | active L/S %d/%d | target %d/%d | avg %.2f MiB" %
                        (100*saved/max(1,total), finished, files, saved/1e9, total/1e9, raw/1e6, useful/1e6,
                         "pending" if eta is None else "%.2f h" % (eta/3600), hub.pacer.status(),
                         lhs, len(active)-lhs, mix.target, args.workers-mix.target, avg/(1024**2)))
                    last = now
        except BaseException:
            hub.stop.set()
            for future in active:
                future.cancel()
            raise


def verify(db, layout, hub, files, check_drive, repaired_only=False):
    source = "files"
    if repaired_only:
        db.execute("CREATE TEMP TABLE repaired AS SELECT id FROM jobs")
        source = "repaired JOIN files ON files.rowid=repaired.id"
    entries = "SELECT files.rowid," + COLUMNS + " FROM " + source
    db.execute("DELETE FROM jobs")
    checked = bad_bytes = 0
    last = time.monotonic()
    failures = layout.path(STATE + "/verification-failures.jsonl")
    with failures.open("w", encoding="utf-8") as log_file:
        for checked, record in enumerate(db.execute(entries), 1):
            key, row = record[0], record[1:]
            if not digest_ok(layout.path(row[1]), row, hub.stop):
                db.execute("INSERT INTO jobs VALUES (?,?)", (key, row[2]))
                bad_bytes += row[2]
                log_file.write(json.dumps(row[0]) + "\n")
            if time.monotonic() - last >= 15:
                check_drive()
                log("Verify %d/%d files" % (checked, files))
                last = time.monotonic()
    count = db.execute("SELECT count(*) FROM jobs").fetchone()[0]
    log("Verified %d/%d files" % (checked-count, files))
    return count, bad_bytes


def run(args):
    stop = threading.Event()
    hub = Hub(args.endpoint, args.repo, stop)
    layout = Layout(destination(args.destination))
    if args.verify_only and not layout.path(STATE + "/manifest.sqlite").is_file():
        raise ValueError("Saved manifest required")
    layout.state.mkdir(parents=True, exist_ok=True)
    identity = layout.root.stat()
    def check_drive():
        if not os.path.samestat(identity, layout.root.stat()):
            raise OSError("Destination volume or directory changed")

    with ExitStack() as stack:
        # An older MDS-Bench process uses this lock in the same output folder.
        legacy = layout.path(".mds-state/run.lock")
        if legacy.is_file() or args.reuse_mds_parts:
            if not legacy.parent.is_dir():
                raise ValueError("No legacy MDS state directory")
            stack.enter_context(single_run(legacy))
        stack.enter_context(single_run(layout.path(STATE + "/run.lock")))
        if args.reuse_mds_parts and not args.verify_only:
            layout.enable_mds_parts(hub.repo, args.revision)
        db = stack.enter_context(closing(sqlite3.connect(str(layout.path(STATE + "/manifest.sqlite")))))
        revision, files, total = manifest(db, hub, args.revision, args.verify_only)
        log("Snapshot %s @ %s | %d files | %.3f GB" % (hub.repo, revision, files, total/1e9))
        report_path = layout.path(STATE + "/verification.json")
        if report_path.exists():
            os.replace(report_path, layout.path(STATE + "/verification.previous.json"))
        db.execute("CREATE TEMP TABLE jobs(id INTEGER PRIMARY KEY,size INTEGER NOT NULL)")
        def transfer(baseline):
            transfer_queue(db, hub, revision, layout, args, files, total, baseline, check_drive)

        if not args.verify_only:
            transfer(build_queue(db, layout, files, total))
        bad, bad_bytes = verify(db, layout, hub, files, check_drive)
        if bad and not args.verify_only:
            log("Repairing %d files" % bad)
            transfer(total - bad_bytes)
            bad, _ = verify(db, layout, hub, bad, check_drive, repaired_only=True)
        check_drive()
        write_report(layout, dict(repository=hub.repo, revision=revision, files=files, bytes=total,
                                  verified=files-bad, missing_or_corrupt=bad, complete=bad == 0,
                                  run_id=os.getenv("HF_ADAPTIVE_RUN_ID"),
                                  verified_at_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
        if bad:
            raise IncompleteDownload("See verification-failures.jsonl")


def main(argv=None):
    try:
        run(parse_args(argv))
    except KeyboardInterrupt:
        log("Stopped. Re-run with the same destination to resume.")
        return 130
    except (TemporaryHubError,) + NETWORK_ERRORS as exc:
        log("Temporary network error: " + (str(exc) if isinstance(exc, TemporaryHubError) else type(exc).__name__))
        return 75
    except IncompleteDownload as exc:
        log(str(exc))
        return 76
    except requests.RequestException as exc:
        response = exc.response
        log("Request failed: " + ("HTTP %d" % response.status_code if response is not None else type(exc).__name__))
        return 2
    except (OSError, ValueError, sqlite3.Error, KeyError) as exc:
        log("Error: " + str(exc))
        return 2
    return 0


if __name__ == "__main__":
    if os.name == "nt":
        signal.signal(signal.SIGBREAK, signal.default_int_handler)
    sys.exit(main())
