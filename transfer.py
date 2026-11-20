"""Download into a resumable partial, verify, then publish with os.replace."""
import os
import re
import threading
from urllib.parse import quote

import requests

from hub import IncompleteDownload, NETWORK_ERRORS, TemporaryHubError, log
from storage import BLOCK, digest_ok, file_size


class Traffic:
    def __init__(self):
        self.lock = threading.Lock()
        self.received = 0
        self.retained = 0

    def record(self, received, retained):
        with self.lock:
            self.received += received
            self.retained += retained

    def snapshot(self):
        with self.lock:
            return self.received, self.retained


def fetch(hub, revision, layout, row, traffic):
    remote, name, size, _, _ = row
    part = layout.part(remote)
    dest = layout.path(name)
    part.parent.mkdir(parents=True, exist_ok=True)
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = hub.endpoint + "/datasets/" + quote(hub.repo) + "/resolve/" + revision + "/" + quote(remote)
    failures = 0
    restart = False
    while True:
        hub.wait(0)
        offset = file_size(part)
        if offset == size and digest_ok(part, row, hub.stop):
            os.replace(part, dest)
            return
        if offset >= 0 and (restart or offset >= size):
            part.unlink()
            traffic.record(0, -min(offset, size))
            offset = 0
        offset = max(0, offset)
        restart = False
        try:
            with hub.get(url, resolver=True, headers={"Range": "bytes=%d-" % offset} if offset else {},
                         stream=True) as response:
                append = response.status_code == 206
                end = size
                if append:
                    span = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", ""))
                    if not span or int(span[1]) != offset or int(span[3]) != size or not offset <= int(span[2]) < size:
                        raise IncompleteDownload("invalid Content-Range")
                    end = int(span[2]) + 1
                elif response.status_code != 200:
                    raise IncompleteDownload("unexpected resolver response")
                written = offset if append else 0
                with part.open("ab" if append and offset else "wb") as output:
                    if not append and offset:
                        traffic.record(0, -offset)
                    for block in response.iter_content(BLOCK):
                        hub.wait(0)
                        written += len(block)
                        if written > end:
                            raise IncompleteDownload("oversized response")
                        output.write(block)
                        traffic.record(len(block), len(block))
            if written == end and end < size:
                continue
            if written == size and digest_ok(part, row, hub.stop):
                os.replace(part, dest)
                return
            reason = "incomplete file or hash mismatch"
            restart = written == size
        except NETWORK_ERRORS as exc:
            reason = type(exc).__name__
            if failures >= 4:
                raise TemporaryHubError("Body retries exhausted for " + remote) from None
        except requests.HTTPError as exc:
            if exc.response.status_code != 416 or not offset:
                raise
            reason, restart = "HTTP 416", True
        except IncompleteDownload as exc:
            reason, restart = str(exc), True
        hub.retry()
        failures += 1
        if failures >= 5:
            raise IncompleteDownload("%s: %s" % (remote, reason))
        log("Retry file %d/4: %s (%s)" % (failures, remote, reason))
        hub.wait(2 ** (failures-1))
