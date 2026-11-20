"""HTTP access and one shared resolver budget per downloader."""
from collections import deque
from email.utils import parsedate_to_datetime
import os
from pathlib import Path
import re
import threading
import time
from urllib.parse import quote, urlsplit

import requests


class TemporaryHubError(RuntimeError):
    pass


class IncompleteDownload(RuntimeError):
    pass


def log(message):
    print(message, flush=True)


class ResolverPacer:
    """Share resolver capacity across workers, including range requests and retries."""
    def __init__(self, target=2970):
        self.target = target
        self.window = 300
        self.starts = deque()
        self.burst = 4
        self.tokens = float(self.burst)
        self.updated = time.monotonic()
        self.lock = threading.Lock()
        self.throttles = 0
        self.policy = None

    def observe_policy(self, header):
        for entry in header.split(','):
            fields = [part.strip() for part in entry.split(';')]
            if '"resolvers"' not in fields:
                continue
            params = dict(part.split('=', 1) for part in fields if '=' in part)
            try:
                quota, window = int(params['q']), int(params['w'])
            except (KeyError, ValueError):
                continue
            if quota <= 0 or window <= 0:
                continue
            with self.lock:
                if self.policy == (quota, window):
                    return
                self.policy = (quota, window)
                self.window = window
                self.target = max(1, quota * 99 // 100)
                log('Hub resolver quota %d/%ds; download target %d/%ds' %
                    (quota, window, self.target, window))
            return

    def prune(self, now):
        while self.starts and self.starts[0] <= now - self.window:
            self.starts.popleft()

    def reserve(self, now):
        # Called under lock. Credits absorb Windows scheduler jitter.
        self.prune(now)
        rate = self.target / self.window
        self.tokens = min(self.burst, self.tokens + (now-self.updated)*rate)
        self.updated = now
        if len(self.starts) >= self.target:
            return max(0.001, self.starts[0] + self.window - now)
        if self.tokens < 1:
            return max(0.001, (1-self.tokens)/rate)
        self.tokens -= 1
        self.starts.append(now)
        return 0

    def status(self):
        with self.lock:
            self.prune(time.monotonic())
            period = '5min' if self.window == 300 else '%ds' % self.window
            return 'resolve/%s %d (target %d) | resolver 429 total %d' % (
                period, len(self.starts), self.target, self.throttles)


def retry_delay(headers):
    value = headers.get('Retry-After', '').strip()
    delays = []
    if value.isdigit():
        delays.append(int(value))
    elif value:
        try:
            delays.append(max(0, parsedate_to_datetime(value).timestamp() - time.time()))
        except (ValueError, TypeError, OverflowError, OSError):
            pass
    reset = re.search(r'(?:^|;)\s*t=(\d+)', headers.get('RateLimit', ''))
    if reset:
        delays.append(int(reset[1]))
    return max(delays) if delays else 60



NETWORK_ERRORS = (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError)
OFFICIAL = "https://huggingface.co"


def token_for(endpoint):
    if endpoint != OFFICIAL:
        return os.getenv("HF_ENDPOINT_TOKEN")
    token = os.getenv("HF_TOKEN")
    if token:
        return token.strip()
    home = Path(os.getenv("HF_HOME") or Path(os.getenv("XDG_CACHE_HOME") or Path.home() / ".cache") / "huggingface")
    try:
        return Path(os.getenv("HF_TOKEN_PATH") or home / "token").read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None


class Hub:
    def __init__(self, endpoint, repo, stop):
        self.endpoint = endpoint.rstrip("/")
        origin = urlsplit(self.endpoint)
        local = origin.hostname in ("localhost", "127.0.0.1", "::1")
        if not (origin.scheme == "https" or local and origin.scheme == "http"):
            raise ValueError("Use an HTTPS endpoint or a loopback HTTP test server")
        if not origin.hostname or origin.username or origin.password or origin.path or origin.query or origin.fragment:
            raise ValueError("Endpoint must be a service origin")
        if not re.fullmatch(r"[\w.-]+(?:/[\w.-]+)?", repo) or any(p in (".", "..") for p in repo.split("/")):
            raise ValueError("Invalid dataset repository ID")
        self.repo = repo
        self.api = self.endpoint + "/api/datasets/" + quote(repo)
        self.stop = stop
        self.token = token_for(self.endpoint)
        self.local = threading.local()
        self.pacer = ResolverPacer()
        self.lock = threading.Lock()
        self.pause_until = 0
        self.retries = 0

    def wait(self, seconds):
        if self.stop.wait(seconds):
            raise InterruptedError("Stopped")

    def retry(self):
        with self.lock:
            self.retries += 1

    def get(self, url, *, resolver=False, **options):
        if urlsplit(url)[:2] != urlsplit(self.endpoint)[:2]:
            raise ValueError("Unexpected pagination origin")
        if not hasattr(self.local, "session"):
            self.local.session = requests.Session()
        headers = dict(options.pop("headers", {}), **{"Accept-Encoding": "identity"})
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        for attempt in range(5):
            while True:
                self.wait(0)
                with self.lock, self.pacer.lock:
                    now = time.monotonic()
                    delay = self.pause_until - now
                    if delay <= 0 and resolver:
                        delay = self.pacer.reserve(now)
                if delay <= 0:
                    break
                self.wait(min(2, delay))
            try:
                response = self.local.session.get(url, headers=headers, timeout=(15, 45), **options)
            except NETWORK_ERRORS as exc:
                reason = type(exc).__name__
            else:
                origin = response.history[0] if response.history else response
                if resolver:
                    self.pacer.observe_policy(origin.headers.get("RateLimit-Policy", ""))
                    limit = origin.headers.get("RateLimit", "")
                    if '"resolvers"' in limit and re.search(r";\s*r=0(?:;|$)", limit):
                        self.pause(retry_delay(origin.headers))
                    if origin.status_code == 429:
                        with self.pacer.lock:
                            self.pacer.throttles += 1
                code = response.status_code
                if code not in (408, 429, 499, 500, 502, 503, 504):
                    try:
                        response.raise_for_status()
                    except requests.HTTPError:
                        response.close()
                        raise
                    return response
                if code == 429 or "Retry-After" in response.headers:
                    self.pause(retry_delay(response.headers))
                response.close()
                reason = "HTTP %d" % code
            self.retry()
            if attempt < 4:
                log("Retry request %d/4 (%s)" % (attempt + 1, reason))
                self.wait(2 ** attempt)
        raise TemporaryHubError("Network retries exhausted (%s)" % reason)

    def signals(self):
        with self.lock, self.pacer.lock:
            return self.retries, self.pacer.throttles, self.pacer.policy

    def pause(self, seconds):
        with self.lock:
            self.pause_until = max(self.pause_until, time.monotonic() + seconds + 1)
