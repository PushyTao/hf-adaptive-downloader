"""Pinned snapshot manifest, local file layout, and integrity checks."""
from contextlib import contextmanager
import ctypes
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from urllib.parse import quote, urljoin, urlsplit

import requests

from hub import log

STATE = ".hf-adaptive"
COLUMNS = "path,local,size,algo,digest"
BLOCK = 1024 * 1024


def destination(value):
    path = Path(value).expanduser().resolve()
    if os.name == "nt" and not str(path).startswith("\\\\?\\"):
        name = str(path)
        path = Path("\\\\?\\UNC\\" + name[2:] if name.startswith("\\\\") else "\\\\?\\" + name)
    return path


def allocation_unit(root):
    if os.name == "nt":
        sectors, sector_bytes = ctypes.c_uint32(), ctypes.c_uint32()
        if not ctypes.windll.kernel32.GetDiskFreeSpaceW(str(root.anchor), ctypes.byref(sectors),
                                                        ctypes.byref(sector_bytes), None, None):
            raise ctypes.WinError()
        return sectors.value * sector_bytes.value
    return os.statvfs(root).f_frsize or 4096


def reject_link(path):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
        raise ValueError("Symlink or reparse point in destination: " + str(path))


class Layout:
    def __init__(self, root):
        self.root = root
        self.state = root / STATE
        self.check_dir = lru_cache(maxsize=65536)(self._check_dir)
        reject_link(self.state)

    def _check_dir(self, path):
        if path != self.root:
            self.check_dir(path.parent)
            reject_link(path)

    def path(self, relative):
        path = self.root / relative
        self.check_dir(path.parent)
        reject_link(path)
        return path

    def part(self, remote):
        key = hashlib.sha256(remote.encode()).hexdigest()
        return self.path(STATE + "/parts/" + key[:2] + "/" + key)


@contextmanager
def single_run(path):
    with path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise ValueError("Another process holds " + str(path)) from exc
        else:
            import fcntl
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ValueError("Another process holds " + str(path)) from exc
        # Closing the handle releases the lock, including after process death.
        yield


def disk_name(path):
    parts = path.split('/')
    if any(p in ('', '.', '..') for p in parts):
        raise ValueError('Unsupported repository path: ' + path)
    out = []
    for p in parts:
        name = ''.join('~%02X~' % ord(c) if c in '<>:"\\|?*~' or ord(c) < 32 or
                       (i >= len(p.rstrip(' .'))) else c for i, c in enumerate(p))
        if re.fullmatch(r'(?i:con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])', p.split('.')[0]):
            name = '~R~' + name
        if len(name.encode('utf-16-le')) > 480:
            name = '~L~' + hashlib.sha256(p.encode()).hexdigest()
        if not out and name.casefold() in (STATE, '.mds-state', '.git'):
            name = '~R~' + name
        out.append(name)
    return '/'.join(out)


def meta(db, key, fallback=None):
    row = db.execute('SELECT val FROM meta WHERE key=?', (key,)).fetchone()
    return row[0] if row else fallback


def put(db, key, val):
    db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (key, str(val)))


def manifest(db, hub, requested, offline=False):
    db.execute("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,val TEXT)")
    db.execute("CREATE TABLE IF NOT EXISTS files(path TEXT PRIMARY KEY,local TEXT,keyname TEXT UNIQUE,"
               "size INTEGER,algo TEXT,digest TEXT)")
    revision = meta(db, "revision")
    if revision:
        if meta(db, "repo") != hub.repo or requested not in (meta(db, "ref"), revision):
            raise ValueError("Destination is bound to a different repository or revision")
    if offline and meta(db, "complete") != "1":
        raise ValueError("Offline verification requires a complete saved manifest")
    if not revision:
        if re.fullmatch("[a-f0-9]{40}", requested):
            revision = requested
        else:
            with hub.get(hub.api + "/revision/" + quote(requested, safe="") + "?expand=sha") as response:
                revision = response.json()["sha"]
        if not re.fullmatch("[a-f0-9]{40}", revision):
            raise ValueError("Invalid snapshot SHA")
        expected = None
        try:
            with hub.get(hub.api + "/treesize/" + revision) as response:
                expected = int(response.json()["size"])
        except requests.HTTPError as exc:
            if exc.response.status_code not in (404, 405, 501):
                raise
        with db:
            put(db, "revision", revision)
            put(db, "ref", requested)
            put(db, "repo", hub.repo)
            if expected is not None:
                put(db, "expected", expected)
    cursor = meta(db, "next", urlsplit(hub.api).path + "/tree/" + revision + "?recursive=true&expand=false&limit=1000")
    pages = int(meta(db, "pages", "0"))
    while meta(db, "complete") != "1":
        url = hub.endpoint + cursor
        with hub.get(url) as response:
            entries = response.json()
            next_link = response.links.get("next", {}).get("url")
        with db:
            for item in entries:
                if item["type"] == "directory":
                    continue
                if item["type"] != "file":
                    raise ValueError("Unexpected tree entry")
                remote = item["path"]
                name = disk_name(remote)
                size = item["size"]
                lfs = item.get("lfs")
                algo, digest = ("sha256", lfs["oid"]) if lfs else ("sha1", item["oid"])
                if not re.fullmatch("[0-9a-f]{%d}" % (64 if lfs else 40), digest) or size < 0:
                    raise ValueError("Invalid tree metadata for " + remote)
                if db.execute("SELECT 1 FROM files WHERE keyname=?", (name.casefold(),)).fetchone():
                    name = "~case~/" + hashlib.sha256(remote.encode()).hexdigest()
                db.execute("INSERT INTO files VALUES (?,?,?,?,?,?)", (remote, name, name.casefold(), size, algo, digest))
            pages += 1
            put(db, "pages", pages)
            if next_link:
                nxt = urlsplit(urljoin(url, next_link))
                if nxt[:2] != urlsplit(hub.endpoint)[:2] or nxt.path != urlsplit(url).path:
                    raise ValueError("Unexpected tree pagination URL")
                cursor = nxt.path + "?" + nxt.query
                if hub.endpoint + cursor == url:
                    raise ValueError("Repeated tree cursor")
                put(db, "next", cursor)
            else:
                size = db.execute("SELECT coalesce(sum(size),0) FROM files").fetchone()[0]
                if size != int(meta(db, "expected", size)):
                    raise ValueError("Tree size mismatch at pinned revision")
                put(db, "complete", 1)
        if pages == 1 or pages % 10 == 0:
            log("Manifest: %d pages" % pages)
    count, total = db.execute("SELECT count(*),coalesce(sum(size),0) FROM files").fetchone()
    return revision, count, total


def file_size(path):
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return -1


def digest_ok(path, row, stop):
    _, _, size, algo, expected = row
    if file_size(path) != size:
        return False
    h = hashlib.new(algo)
    if algo == 'sha1':
        h.update(('blob %d\0' % size).encode())
    with path.open('rb') as f:
        for block in iter(lambda: f.read(BLOCK), b''):
            if stop.is_set():
                raise InterruptedError('Stopped')
            h.update(block)
    return h.hexdigest() == expected


def write_report(layout, report):
    temp = layout.path(STATE + "/verification.tmp")
    temp.write_text(json.dumps(report, indent=2), encoding="utf-8")
    os.replace(temp, layout.path(STATE + "/verification.json"))
