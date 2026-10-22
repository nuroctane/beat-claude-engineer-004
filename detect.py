"""Ingest-stage validator for the Engineer 004 event fixture.

This is the logic I'd run in the stream processor between Kinesis and the
stores: parse, normalize, dedupe, quarantine, flag. It runs against the
fixture so every finding is tied to a real event_id.

Usage:
    python detect.py fixtures/event_sample.jsonl
    python detect.py fixtures/event_sample.jsonl --json findings.json
    python detect.py - < events.jsonl                 # stdin
    python detect.py fixtures/event_sample.jsonl --fail-on quarantine,compliance
    python detect.py fixtures/event_sample.jsonl --check-upstream

Stdlib only. Python 3.10+.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

__version__ = "1.3.0"

# sha256 of the fixture exactly as published (LF line endings). A Windows
# checkout with core.autocrlf rewrites it to CRLF and changes the hash, so
# the report also carries an LF-normalized hash and matches on either.
EXPECTED_SHA256 = "1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235"
CRLF, LF = bytes([13, 10]), bytes([10])
UPSTREAM_URL = ("https://raw.githubusercontent.com/ericosiu/beat-claude/main/"
                "challenges/engineer-004/fixtures/event_sample.jsonl")

KNOWN_TYPES = {"page_view", "click", "form_submit", "custom", "identify"}
# Legacy SDK shapes we must accept forever: the brief says no SDK changes.
LEGACY_TYPE_MAP = {"pageview": "page_view"}
LEGACY_FIELD_MAP = {"timestamp": "ts"}
LEGACY_PROP_MAP = {"page_path": "path", "ref": "referrer"}

CLOCK_TOLERANCE = timedelta(seconds=5)
IMPLAUSIBLE_SKEW = timedelta(days=1)
# Offline SDKs buffer for days, not years. Older than this is a clock problem.
MAX_LATE = timedelta(days=30)
BOT_BURST_EVENTS = 4
BOT_BURST_WINDOW = timedelta(seconds=1)
# Whole host labels only: scanner.example-bot.net counts, robotics.com and abbott.com don't.
BOT_HOST = re.compile(r"(?:^|[.-])(?:bot|bots|crawl|crawler|scan|scanner|spider)(?:[.-]|$)", re.I)

EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
# Phone shapes only: a leading + or a NANP layout. A bare digit run is an
# order id or a date far more often than a phone number.
PHONE_INTL = re.compile(r"\+\d[\d\s().-]{7,}\d")
PHONE_NANP = re.compile(r"\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}")
# Key hints match on the key's last word, so contact_email and ip_address count
# but email_campaign_id, iphone_model and adobe_campaign don't.
PII_KEY_TAILS = {"email", "mail", "phone", "mobile", "ssn", "street", "address",
                 "dob", "birthday", "birthdate", "surname", "firstname", "lastname", "fullname"}
PII_KEY_PAIRS = {("first", "name"), ("last", "name"), ("full", "name"), ("birth", "date")}
CLIENT_COUNTER_KEY = re.compile(r"(^count_|_count$)", re.I)

# "key": "value" pairs in a line too broken to parse, so key-hinted PII can still be masked.
RAW_KV = re.compile(r'"((?:[^"\\]|\\.)*)"(\s*:\s*)"((?:[^"\\]|\\.)*)"')
EVENT_ID_RAW = re.compile(r'"event_id"\s*:\s*"([^"]+)"')
TENANT_RAW = re.compile(r'"tenant_id"\s*:\s*"([^"]+)"')

# What the pipeline does with each class. The action is the contract; the
# handling text is the why.
ACTIONS = {
    "INVALID_ENCODING": "quarantine",
    "MALFORMED_JSON": "quarantine",
    "NOT_AN_OBJECT": "quarantine",
    "MISSING_EVENT_ID": "quarantine",
    "MISSING_TENANT": "quarantine",
    "BAD_IDENTIFIER": "repair",
    "PRIVACY_REQUEST_VIA_SDK": "compliance",
    "DELETION_SCOPE": "compliance",
    "PII_IN_PAYLOAD": "compliance",
    "DUPLICATE_RETRY": "repair",
    "LEGACY_SCHEMA": "repair",
    "PROPERTIES_NOT_OBJECT": "repair",
    "MISSING_RECEIVED_AT": "repair",
    "CLOCK_SKEW_FUTURE": "repair",
    "TIMESTAMP_IMPLAUSIBLE": "repair",
    "TIMESTAMP_UNPARSEABLE": "repair",
    "TIMESTAMP_NO_TZ": "repair",
    "BOT_SUSPECTED": "exclude",
    "CLIENT_COMPUTED_METRIC": "exclude",
    "CLIENT_COUNTER_MISMATCH": "exclude",
    "EVENT_ID_COLLISION": "alert",
    "UNKNOWN_EVENT_TYPE": "alert",
    "ANON_MULTI_USER": "alert",
    "LATE_ARRIVAL": "info",
}
ACTION_ORDER = ["quarantine", "compliance", "repair", "exclude", "alert", "info"]

HANDLING = {
    "INVALID_ENCODING": "Dead-letter the raw bytes. Never transcode by guessing.",
    "MALFORMED_JSON": "Dead-letter the raw bytes with the tenant hint; never repair by "
                      "guessing. Count per tenant so a broken SDK build shows up as a rate.",
    "NOT_AN_OBJECT": "Dead-letter. Valid JSON that isn't an event is a client bug, not data.",
    "MISSING_EVENT_ID": "Quarantine. Without an id there is no idempotency key, so a "
                        "retry would double count.",
    "MISSING_TENANT": "Quarantine. No tenant means no partition, no retention policy, no "
                      "deletion owner. Do not infer tenant from path or referrer.",
    "BAD_IDENTIFIER": "Keep the event, drop the unusable id. Numbers are read as strings; "
                      "objects and lists can't be stitched to anyone.",
    "PRIVACY_REQUEST_VIA_SDK": "Do not execute from the event stream: it is unauthenticated "
                               "client input. Route to the compliance queue; the tenant "
                               "confirms through the authenticated deletion API.",
    "DELETION_SCOPE": "Pre-identify anonymous history is personal data once stitched. "
                      "Deletion walks the identity graph, not just user_id.",
    "PII_IN_PAYLOAD": "Hash or drop at the collector before Kinesis, per tenant policy. Raw "
                      "PII in an append-only log turns every deletion into a rewrite.",
    "DUPLICATE_RETRY": "Drop the later copy. Idempotency key is (tenant_id, event_id); the "
                       "first copy seen wins. Counted, not silently eaten.",
    "LEGACY_SCHEMA": "Normalize through a versioned mapping table. Old SDKs stay in the "
                     "field forever; rejecting them is data loss.",
    "PROPERTIES_NOT_OBJECT": "Keep the value under properties._raw and alert the SDK owner.",
    "MISSING_RECEIVED_AT": "The collector stamps received_at at ingest. This record never "
                           "had one, so its event time is client-only and untrusted.",
    "CLOCK_SKEW_FUTURE": "An event can't happen after we received it. Bucket on received_at "
                         "past tolerance; keep client ts for ordering inside a session.",
    "TIMESTAMP_IMPLAUSIBLE": "Bucket on received_at; keep client ts as an attribute only.",
    "TIMESTAMP_UNPARSEABLE": "Bucket on received_at; keep the original string.",
    "TIMESTAMP_NO_TZ": "Read as UTC and flag. A naive timestamp from a browser is local time "
                       "more often than not.",
    "BOT_SUSPECTED": "Keep raw, tag is_bot, exclude from dashboards, segments and "
                     "personalization triggers. Never delete on a heuristic.",
    "CLIENT_COMPUTED_METRIC": "Store as reported, never segment on it. Segments like "
                              "'viewed pricing 3x' are computed server-side.",
    "CLIENT_COUNTER_MISMATCH": "Not proof the client is wrong: the sample is a slice of the "
                               "day. Proof the number can't be a trigger input.",
    "EVENT_ID_COLLISION": "Keep both and alert: a buggy id generator or a replay with "
                          "mutation. Not safe to dedupe.",
    "UNKNOWN_EVENT_TYPE": "Store raw, exclude from aggregates, alert the owning team.",
    "ANON_MULTI_USER": "Shared device or a bad identify call. Keep the history, never "
                       "auto-merge two known users.",
    "LATE_ARRIVAL": "Accept into raw and the warehouse. Real-time windows are closed, so it "
                    "lands in the next reconciliation pass.",
}
assert ACTIONS.keys() == HANDLING.keys()


@dataclass
class Finding:
    anomaly: str
    event_id: str | None
    line: int
    detail: str
    action: str = ""
    handling: str = ""

    def __post_init__(self):
        self.action = self.action or ACTIONS[self.anomaly]
        self.handling = self.handling or HANDLING[self.anomaly]


@dataclass
class Event:
    line: int
    event_id: str | None
    tenant_id: str | None
    type: str | None
    raw: str  # redacted source line, safe to publish
    unique: bool = True


@dataclass
class Report:
    sha256: str
    sha256_lf: str
    lines: int
    parsed: int
    unique_events: int
    version: str = __version__
    findings: list[Finding] = field(default_factory=list)
    events: list[Event] = field(default_factory=list)
    identity_graph: dict[str, list[str]] = field(default_factory=dict)
    clean_event_ids: list[str] = field(default_factory=list)
    summary: dict = field(default_factory=dict)


def parse_ts(value) -> tuple[datetime | None, str | None]:
    """Return (aware UTC datetime, problem). problem is None, 'no_tz' or 'unparseable'."""
    if value is None or value == "":
        return None, None
    if isinstance(value, bool):
        return None, "unparseable"
    if isinstance(value, (int, float)):
        seconds = value / 1000 if abs(value) > 1e11 else value  # epoch ms vs s
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc), None
        except (OverflowError, OSError, ValueError):
            return None, "unparseable"
    if not isinstance(value, str):
        return None, "unparseable"
    try:
        dt = datetime.fromisoformat(_iso(value))
    except ValueError:
        return None, "unparseable"
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc), "no_tz"
    return dt.astimezone(timezone.utc), None


def _iso(value: str) -> str:
    """Shapes Python 3.11+ accepts but 3.10 doesn't, so every version agrees:
    a trailing Z, a +0000 offset, and fractions that aren't 3 or 6 digits."""
    s = value.strip()
    if s[-1:] in ("Z", "z"):
        s = s[:-1] + "+00:00"
    s = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", s)
    return re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], s, count=1)


def span(td: timedelta) -> str:
    secs = abs(td.total_seconds())
    if secs < 120:
        return f"{secs:.1f}s"
    if secs < 2 * 3600:
        return f"{secs / 60:.1f} min"
    if secs < 2 * 86400:
        return f"{secs / 3600:.1f} h"
    return f"{secs / 86400:.0f} days"


def bot_referrer(ref) -> bool:
    if not isinstance(ref, str) or not ref.strip():
        return False
    host = urlparse(ref if "//" in ref else "//" + ref).hostname or ""
    return bool(BOT_HOST.search(host))


def looks_like_pii(value: str) -> bool:
    v = value.strip()
    return bool(EMAIL.search(v) or PHONE_INTL.fullmatch(v) or PHONE_NANP.fullmatch(v))


def pii_key(key) -> bool:
    if not isinstance(key, str):
        return False
    words = re.findall(r"[a-z]+", re.sub(r"([a-z])([A-Z])", r"\1_\2", key).lower())
    return bool(words) and (words[-1] in PII_KEY_TAILS or tuple(words[-2:]) in PII_KEY_PAIRS)


def is_pii(key, value) -> bool:
    return isinstance(value, str) and bool(value.strip()) and (pii_key(key) or looks_like_pii(value))


def scan_pii(obj, path: str = "") -> list[tuple[str, str]]:
    """Walk nested properties. A key hint only counts when it holds a real string."""
    hits = []
    items = obj.items() if isinstance(obj, dict) else enumerate(obj) if isinstance(obj, list) else []
    for k, v in items:
        p = f"{path}[{k}]" if isinstance(k, int) else (f"{path}.{k}" if path else str(k))
        if isinstance(v, (dict, list)):
            hits += scan_pii(v, p)
        elif is_pii(k, v):
            hits.append((p, v))
    return hits


def redact(value: str) -> str:
    """Mask emails and phone numbers inside free text, such as a line we can't parse."""
    value = EMAIL.sub(lambda m: m.group(0)[0] + "***@" + m.group(0).split("@")[1], value)
    value = PHONE_INTL.sub(lambda m: "***" + m.group(0)[-4:], value)
    return PHONE_NANP.sub(lambda m: "***" + m.group(0)[-4:], value)


def redact_text(line: str) -> str:
    """For a line we can't parse: mask emails and phones, plus values under PII keys."""
    line = RAW_KV.sub(lambda m: f'"{m.group(1)}"{m.group(2)}"{mask(m.group(3))}"'
                      if pii_key(m.group(1)) and m.group(3).strip() else m.group(0), line)
    return redact(line)


def mask(value: str) -> str:
    """Mask one PII value: enough to prove the finding, not enough to republish it."""
    masked = redact(value)
    return masked if masked != value else (value.strip()[:1] + "***")


def mask_obj(obj):
    """Copy of a parsed record with every PII value masked, at any depth."""
    if isinstance(obj, dict):
        return {k: mask(v) if is_pii(k, v) else mask_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [mask(v) if is_pii(None, v) else mask_obj(v) for v in obj]
    return obj


def canonical_id(value) -> str | None:
    """Ids are strings. Integers from older SDKs are read as strings; anything else is unusable."""
    if isinstance(value, str):
        return value or None
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return None


def _safe_id(value) -> str | None:
    cid = canonical_id(value)
    return redact(cid) if cid else None


def normalize(rec: dict) -> tuple[dict, list[str], bool]:
    """Map legacy SDK shapes onto the current schema.
    Returns (event, legacy changes, properties_was_bad)."""
    changes = []
    out = dict(rec)
    for old, new in LEGACY_FIELD_MAP.items():
        if old in out and new not in out:
            out[new] = out.pop(old)
            changes.append(f"{old}->{new}")
    if out.get("type") in LEGACY_TYPE_MAP:
        changes.append(f"type {out['type']}->{LEGACY_TYPE_MAP[out['type']]}")
        out["type"] = LEGACY_TYPE_MAP[out["type"]]
    raw_props = out.get("properties")
    bad_props = raw_props is not None and not isinstance(raw_props, dict)
    props = dict(raw_props) if isinstance(raw_props, dict) else {}
    if bad_props:
        props["_raw"] = raw_props
    for old, new in LEGACY_PROP_MAP.items():
        if old in props and new not in props:
            props[new] = props.pop(old)
            changes.append(f"properties.{old}->{new}")
    out["properties"] = props
    return out, changes, bad_props


def analyze_bytes(raw: bytes) -> Report:
    report = Report(sha256=hashlib.sha256(raw).hexdigest(),
                    sha256_lf=hashlib.sha256(raw.replace(CRLF, LF)).hexdigest(),
                    lines=0, parsed=0, unique_events=0)
    flagged_lines: set[int] = set()

    def add(anomaly, event_id, line, detail):
        # Ids and request text can carry an email; nothing leaves unmasked.
        report.findings.append(Finding(anomaly, redact(event_id) if event_id else None,
                                       line, redact(detail)))
        flagged_lines.add(line)

    # Pass 0: bytes to records. One bad line never takes down the batch.
    records: list[tuple[int, dict]] = []
    for line_no, chunk in enumerate(raw.split(b"\n"), start=1):
        chunk = chunk.rstrip(b"\r")
        if line_no == 1:
            chunk = chunk.removeprefix(b"\xef\xbb\xbf")
        if not chunk.strip():
            continue
        report.lines += 1
        try:
            line = chunk.decode("utf-8")
        except UnicodeDecodeError as exc:
            report.events.append(Event(line_no, None, None, None,
                                       redact_text(chunk.decode("utf-8", "replace")), unique=False))
            add("INVALID_ENCODING", None, line_no, f"byte {exc.start}: {exc.reason}")
            continue
        safe = redact_text(line)
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as exc:
            m, t = EVENT_ID_RAW.search(line), TENANT_RAW.search(line)
            eid = m.group(1) if m else None
            report.events.append(Event(line_no, redact(eid) if eid else None, t.group(1) if t else None,
                                       None, safe, unique=False))
            add("MALFORMED_JSON", eid, line_no, f"{exc.msg} at col {exc.colno}")
            continue
        if not isinstance(rec, dict):
            safe = json.dumps(mask_obj(rec), ensure_ascii=False, separators=(",", ":"))
            report.events.append(Event(line_no, None, None, None, safe, unique=False))
            add("NOT_AN_OBJECT", None, line_no, f"top-level {type(rec).__name__}")
            continue
        report.parsed += 1
        # Re-serialized from the masked record, so key-hinted PII (names,
        # addresses) is masked too, not just what looks like an email or phone.
        safe = json.dumps(mask_obj(rec), ensure_ascii=False, separators=(",", ":"))
        report.events.append(Event(line_no, _safe_id(rec.get("event_id")),
                                   _safe_id(rec.get("tenant_id")),
                                   rec.get("type") if isinstance(rec.get("type"), str) else None,
                                   safe))
        records.append((line_no, rec))
    event_at = {e.line: e for e in report.events}

    # Pass 1: dedupe on (tenant_id, event_id). Same payload = SDK retry.
    seen: dict[tuple, tuple[int, dict]] = {}
    unique: list[tuple[int, dict]] = []
    for line_no, rec in records:
        eid = canonical_id(rec.get("event_id"))
        if eid is None:
            add("MISSING_EVENT_ID", None, line_no, f"event_id={rec.get('event_id')!r}")
            unique.append((line_no, rec))
            continue
        key = (canonical_id(rec.get("tenant_id")), eid)
        if key in seen:
            first_line, first = seen[key]
            strip = lambda r: {k: v for k, v in r.items() if k != "received_at"}
            if strip(first) == strip(rec):
                add("DUPLICATE_RETRY", eid, line_no,
                    f"identical to line {first_line} except received_at "
                    f"({first.get('received_at')} vs {rec.get('received_at')})")
                event_at[line_no].unique = False
                continue
            add("EVENT_ID_COLLISION", eid, line_no, f"same id as line {first_line}, different payload")
        else:
            seen[key] = (line_no, rec)
        unique.append((line_no, rec))
    report.unique_events = len(unique)

    # Pass 2: per-event checks.
    normalized: list[tuple[int, dict, datetime | None]] = []
    for line_no, rec in unique:
        eid = canonical_id(rec.get("event_id"))
        ev, changes, bad_props = normalize(rec)
        if changes:
            add("LEGACY_SCHEMA", eid, line_no, ", ".join(changes))
        if bad_props:
            add("PROPERTIES_NOT_OBJECT", eid, line_no, f"properties is {type(rec['properties']).__name__}")
        raw_tenant = ev.get("tenant_id")
        for k in ("tenant_id", "user_id", "anonymous_id"):
            original, ev[k] = ev.get(k), canonical_id(ev.get(k))
            if k != "tenant_id" and ev[k] is None and original not in (None, ""):
                add("BAD_IDENTIFIER", eid, line_no, f"{k} is {type(original).__name__}")
        if not ev["tenant_id"]:
            add("MISSING_TENANT", eid, line_no, f"tenant_id={raw_tenant!r}")
        props = ev["properties"]
        if ev.get("type") == "privacy_request":
            request, regulation = props.get("request"), props.get("regulation")
            add("PRIVACY_REQUEST_VIA_SDK", eid, line_no,
                f"{request or 'unspecified request'}{' under ' + str(regulation) if regulation else ''} "
                f"for {ev.get('user_id') or ev.get('anonymous_id') or 'no subject'}")
        elif ev.get("type") not in KNOWN_TYPES:
            add("UNKNOWN_EVENT_TYPE", eid, line_no, f"type={ev.get('type')!r}")

        ts, ts_problem = parse_ts(ev.get("ts"))
        recv, recv_problem = parse_ts(ev.get("received_at"))
        for name, problem in (("ts", ts_problem), ("received_at", recv_problem)):
            if problem == "unparseable":
                add("TIMESTAMP_UNPARSEABLE", eid, line_no, f"{name}={ev.get(name)!r}")
            elif problem == "no_tz":
                add("TIMESTAMP_NO_TZ", eid, line_no, f"{name}={ev.get(name)!r} read as UTC")
        if recv is None and recv_problem is None:
            add("MISSING_RECEIVED_AT", eid, line_no, "no server receive timestamp")
        if ts and recv:
            skew = ts - recv
            if skew > IMPLAUSIBLE_SKEW:
                add("TIMESTAMP_IMPLAUSIBLE", eid, line_no, f"client ts {ev['ts']} is {span(skew)} after receipt")
            elif skew > CLOCK_TOLERANCE:
                add("CLOCK_SKEW_FUTURE", eid, line_no, f"client ts {span(skew)} after receipt")
            elif -skew > MAX_LATE:
                add("TIMESTAMP_IMPLAUSIBLE", eid, line_no, f"client ts {ev['ts']} is {span(skew)} before receipt")
            elif -skew > IMPLAUSIBLE_SKEW:
                add("LATE_ARRIVAL", eid, line_no, f"arrived {span(skew)} after the event")

        # The whole event, not just properties: SDKs also put traits and context at the top.
        pii = scan_pii(ev)
        if pii:
            shown = ", ".join(f"{p}={mask(v)}" for p, v in pii)
            add("PII_IN_PAYLOAD", eid, line_no, f"{len(pii)} field(s): {shown}")
        counters = [k for k in props if CLIENT_COUNTER_KEY.search(str(k))]
        if counters:
            add("CLIENT_COMPUTED_METRIC", eid, line_no,
                f"client-supplied {', '.join(f'{k}={props[k]}' for k in counters)}")
        # Server time when we have it. Client time is spoofable and skewed.
        normalized.append((line_no, ev, recv or ts))

    # Pass 3: identity graph per tenant, from any event carrying both ids.
    links: dict[tuple, set] = defaultdict(set)
    anon_users: dict[tuple, dict] = defaultdict(dict)
    for line_no, ev, _ in normalized:
        t, u, a = ev.get("tenant_id"), ev.get("user_id"), ev.get("anonymous_id")
        if t and u and a:
            links[(t, u)].add(a)
            anon_users[(t, a)].setdefault(u, (line_no, ev.get("event_id")))
    report.identity_graph = {redact(f"{t}/{u}"): sorted(redact(x) for x in a)
                             for (t, u), a in sorted(links.items())}
    for (t, a), users in anon_users.items():
        if len(users) > 1:
            line_no, eid = max(users.values())
            add("ANON_MULTI_USER", eid, line_no, f"{t}/{a} identified as {sorted(users)}")

    def identity_of(ev) -> tuple:
        t, u, a = ev.get("tenant_id"), ev.get("user_id"), ev.get("anonymous_id")
        if u:
            return (t, u)
        users = anon_users.get((t, a), {})
        return (t, next(iter(users))) if len(users) == 1 else (t, a)

    # Pass 4: cross-event checks.
    by_actor: dict[tuple, list] = defaultdict(list)
    for line_no, ev, when in normalized:
        # No anonymous id means no single browser to pin a burst on.
        if ev.get("type") == "page_view" and when and ev.get("anonymous_id"):
            by_actor[(ev.get("tenant_id"), ev.get("anonymous_id"))].append((when, line_no, ev))
    for actor, views in by_actor.items():
        views.sort(key=lambda v: v[0])
        burst = any(views[i + BOT_BURST_EVENTS - 1][0] - views[i][0] <= BOT_BURST_WINDOW
                    for i in range(len(views) - BOT_BURST_EVENTS + 1))
        ref_hit = any(bot_referrer(v[2]["properties"].get("referrer")) for v in views)
        if burst or ref_hit:
            why = []
            if burst:
                why.append(f">={BOT_BURST_EVENTS} page_views within {BOT_BURST_WINDOW.total_seconds():.0f}s")
            if ref_hit:
                why.append("scanner-like referrer")
            for _, line_no, ev in views:
                add("BOT_SUSPECTED", ev.get("event_id"), line_no, f"actor {actor[0]}/{actor[1]}: {'; '.join(why)}")

    # Client counters vs what the server saw for the same person, same UTC day.
    for line_no, ev, when in normalized:
        props = ev["properties"]
        name = str(props.get("name") or "")
        if not (name.startswith("viewed_") and "count_today" in props and when):
            continue
        try:
            claimed = int(props["count_today"])
        except (TypeError, ValueError):
            continue
        page = "/" + name.removeprefix("viewed_")
        who = identity_of(ev)
        if who[1] is None:
            continue
        seen_views = [e.get("event_id") for _, e, w in normalized
                      if e.get("type") == "page_view" and identity_of(e) == who
                      and e["properties"].get("path") == page and w and w.date() == when.date()]
        if len(seen_views) != claimed:
            add("CLIENT_COUNTER_MISMATCH", ev.get("event_id"), line_no,
                f"client says {claimed} views of {page}; sample has {len(seen_views)} "
                f"({', '.join(seen_views) or 'none'}) for {who[0]}/{who[1]}")

    # Deletion scope: what a verified delete for this subject must touch.
    for line_no, ev, _ in normalized:
        if ev.get("type") != "privacy_request":
            continue
        t, u, a = ev.get("tenant_id"), ev.get("user_id"), ev.get("anonymous_id")
        if not t or not (u or a):
            add("DELETION_SCOPE", ev.get("event_id"), line_no,
                "cannot scope: request names no tenant or no subject. Nothing is deleted.")
            continue
        anons = set(links.get((t, u), set())) if u else set()
        if a:
            anons.add(a)
        # A browser more than one known user signed in on is a shared device.
        # Its anonymous history can't be attributed to one person, so a person decides.
        shared = {x: sorted(set(anon_users[(t, x)]) - {u}) for x in anons
                  if len(anon_users.get((t, x), {})) > 1}
        touched, held = [], []
        for _, e, _ in normalized:
            if e.get("tenant_id") != t or e is ev:
                continue
            if u and e.get("user_id") == u:
                touched.append(e.get("event_id"))
            elif e.get("user_id"):
                continue  # another known person's event: never in scope
            elif e.get("anonymous_id") in anons:
                (held if e.get("anonymous_id") in shared else touched).append(e.get("event_id"))
        detail = (f"verified delete for {t}/{u or a} must cover anon ids {sorted(anons)} "
                  f"-> events {touched}")
        if shared:
            others = "; ".join(f"{x} also used by {', '.join(us)}" for x, us in sorted(shared.items()))
            detail += f". Shared device ({others}): held for a person {held}"
        add("DELETION_SCOPE", ev.get("event_id"), line_no, detail)

    report.clean_event_ids = [redact(ev["event_id"]) if ev.get("event_id") else None
                              for line_no, ev, _ in normalized if line_no not in flagged_lines]
    by_class = Counter(f.anomaly for f in report.findings)
    report.summary = {
        "by_class": dict(sorted(by_class.items())),
        "by_action": {a: sum(n for c, n in by_class.items() if ACTIONS[c] == a)
                      for a in ACTION_ORDER},
        "fixture_matches": EXPECTED_SHA256 in (report.sha256, report.sha256_lf),
        "line_endings": "crlf" if report.sha256 != report.sha256_lf else "lf",
    }
    order = list(ACTIONS)
    report.findings.sort(key=lambda f: (f.line, ACTION_ORDER.index(f.action), order.index(f.anomaly)))
    return report


def analyze(path: Path) -> Report:
    return analyze_bytes(path.read_bytes())


def check_upstream(timeout: float = 10.0) -> tuple[bool, str]:
    """Fixtures rotate between hiring rounds. Confirm we're answering the live one."""
    import urllib.request
    try:
        with urllib.request.urlopen(UPSTREAM_URL, timeout=timeout) as resp:
            live = hashlib.sha256(resp.read()).hexdigest()
    except OSError as exc:
        return False, f"could not fetch upstream fixture: {exc}"
    if live == EXPECTED_SHA256:
        return True, f"upstream fixture unchanged ({live[:12]})"
    return False, f"upstream fixture ROTATED: live {live[:12]} vs built against {EXPECTED_SHA256[:12]}"


def _cell(text) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def render_markdown(report: Report) -> str:
    ok = "matches" if report.summary["fixture_matches"] else "DOES NOT MATCH"
    out = [
        f"fixture sha256 {report.sha256_lf} ({ok} the version this was built against"
        + (", CRLF copy, hashed as LF)" if report.summary["line_endings"] == "crlf" else ")"),
        f"lines {report.lines} | parsed {report.parsed} | unique events {report.unique_events} "
        f"| findings {len(report.findings)} | clean {len(report.clean_event_ids)}",
        "by action: " + ", ".join(f"{a} {n}" for a, n in report.summary["by_action"].items() if n),
        "",
        "| line | event_id | action | anomaly | detail |",
        "|---:|---|---|---|---|",
    ]
    for f in report.findings:
        out.append(f"| {f.line} | {_cell(f.event_id or '?')} | {f.action} | {f.anomaly} | {_cell(f.detail)} |")
    out += ["", "Identity graph (tenant/user -> anonymous ids):"]
    out += [f"  {k}: {', '.join(v)}" for k, v in report.identity_graph.items()]
    out += ["", f"Clean events: {', '.join(str(e) for e in report.clean_event_ids)}"]
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("path", help="JSONL file, or - for stdin")
    ap.add_argument("--json", type=Path, help="also write the full report as JSON")
    ap.add_argument("--fail-on", default="",
                    help=f"comma list of actions that make the exit code 1: {','.join(ACTION_ORDER)}")
    ap.add_argument("--check-upstream", action="store_true",
                    help="fetch the live challenge fixture and compare checksums")
    ap.add_argument("--version", action="version", version=__version__)
    args = ap.parse_args(argv)

    fail_on = {a.strip() for a in args.fail_on.split(",") if a.strip()}
    unknown = fail_on - set(ACTION_ORDER)
    if unknown:
        ap.error(f"unknown action(s) for --fail-on: {', '.join(sorted(unknown))}")

    # Event text can hold anything; a Windows console shouldn't crash on it.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    try:
        raw = sys.stdin.buffer.read() if args.path == "-" else Path(args.path).read_bytes()
    except OSError as exc:
        ap.error(f"can't read {args.path}: {exc.strerror or exc}")
    report = analyze_bytes(raw)
    print(render_markdown(report))
    if args.json:
        try:
            args.json.write_text(json.dumps(asdict(report), indent=2) + "\n", encoding="utf-8")
        except OSError as exc:
            print(f"can't write {args.json}: {exc.strerror or exc}", file=sys.stderr)
            return 2

    code = 0
    if args.check_upstream:
        ok, msg = check_upstream()
        print(f"\n{msg}", file=sys.stderr)
        code = code or (0 if ok else 2)
    if any(report.summary["by_action"].get(a) for a in fail_on):
        code = code or 1
    return code


if __name__ == "__main__":
    sys.exit(main())
