"""Build the interactive report into site/public.

Everything on the page is rendered here from detect.py and cost_model.py, so
the page can't drift from the code. The test suite runs first and the build
fails if it does. JS on the page only adds behavior; with JS off it still
reads top to bottom.

Usage: python build_site.py
"""

from __future__ import annotations

import io
import json
import shutil
import sys
import unittest
from collections import Counter
from dataclasses import asdict
from html import escape
from pathlib import Path

import cost_model
from detect import ACTION_ORDER, EXPECTED_SHA256, __version__, analyze

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "site" / "src"
OUT = ROOT / "site" / "public"
FIXTURE = ROOT / "fixtures" / "event_sample.jsonl"
REPO = "https://github.com/nuroctane/beat-claude-engineer-004"
SITE = "https://beat-claude-engineer-004.civetadeicollegium.workers.dev"
PDF = ROOT / "SUBMISSION.pdf"
DESCRIPTION = ("Beat Claude Engineer 004: a real-time analytics pipeline for 50M events a day, "
               "argued line by line from the challenge fixture.")
CHALLENGE = "https://github.com/ericosiu/beat-claude/tree/main/challenges/engineer-004"

LABELS = {
    "INVALID_ENCODING": "bad encoding",
    "MALFORMED_JSON": "malformed json",
    "NOT_AN_OBJECT": "not an object",
    "MISSING_EVENT_ID": "no event id",
    "MISSING_TENANT": "no tenant",
    "PRIVACY_REQUEST_VIA_SDK": "deletion via sdk",
    "DELETION_SCOPE": "deletion scope",
    "PII_IN_PAYLOAD": "pii",
    "DUPLICATE_RETRY": "duplicate retry",
    "LEGACY_SCHEMA": "legacy schema",
    "PROPERTIES_NOT_OBJECT": "bad properties",
    "MISSING_RECEIVED_AT": "no receive time",
    "CLOCK_SKEW_FUTURE": "clock ahead",
    "TIMESTAMP_IMPLAUSIBLE": "year off",
    "TIMESTAMP_UNPARSEABLE": "bad timestamp",
    "TIMESTAMP_NO_TZ": "no timezone",
    "BOT_SUSPECTED": "bot",
    "CLIENT_COMPUTED_METRIC": "client counter",
    "CLIENT_COUNTER_MISMATCH": "counter mismatch",
    "EVENT_ID_COLLISION": "id collision",
    "UNKNOWN_EVENT_TYPE": "unknown type",
    "ANON_MULTI_USER": "shared device",
    "LATE_ARRIVAL": "late",
}

ACTION_NOTES = {
    "quarantine": "held aside, never guessed at",
    "compliance": "a person decides",
    "repair": "fixed in flight, original kept",
    "exclude": "stored, kept out of segments",
    "alert": "kept, someone gets paged",
    "info": "noted",
}

# Which keys in the pretty JSON to mark for each class.
MARK_KEYS = {
    "DUPLICATE_RETRY": {"received_at"},
    "CLOCK_SKEW_FUTURE": {"ts", "received_at"},
    "TIMESTAMP_IMPLAUSIBLE": {"ts"},
    "TIMESTAMP_NO_TZ": {"ts", "received_at"},
    "TIMESTAMP_UNPARSEABLE": {"ts", "received_at"},
    "LEGACY_SCHEMA": {"type", "timestamp", "page_path", "ref"},
    "MISSING_TENANT": {"tenant_id"},
    "MISSING_EVENT_ID": {"event_id"},
    "BOT_SUSPECTED": {"ts", "received_at", "referrer", "ref"},
    "PRIVACY_REQUEST_VIA_SDK": {"type", "request", "regulation"},
    "DELETION_SCOPE": {"user_id", "anonymous_id"},
    "CLIENT_COMPUTED_METRIC": {"count_today"},
    "CLIENT_COUNTER_MISMATCH": {"name"},
    "UNKNOWN_EVENT_TYPE": {"type"},
    "PROPERTIES_NOT_OBJECT": {"properties"},
}

FACE_VALUE = [
    ("evt-0017", 18, "The deletion request.",
     "Anyone with a browser console can send it. Executing it means anyone can wipe any "
     "user's history. It opens a verified request; it isn't one."),
    ("evt-0019", 20, "<code>count_today: 3</code>.",
     "The sample shows one pricing view. That doesn't prove the client wrong in a 25-row "
     "slice. It proves the number can't drive a personalization trigger."),
    ("anon-8fc", 13, "The bot verdict.",
     "Four views in 50ms from a scanner referrer is a strong heuristic, not proof. It could "
     "be a customer's own uptime monitor. Tag and exclude, never delete."),
    ("evt-0006", 7, "The 65-minute skew.",
     "Exactly 1h05m with matching milliseconds looks like a timezone bug, not drift. One "
     "event isn't enough to write a correction rule. Until then, received_at."),
    ("n = 25", None, "Any rate from this sample.",
     "Four of 23 unique events are bot-tagged. Calling that 17% bot traffic would be "
     "making it up."),
]

PHASES = [
    ("Weeks 1 to 4", "Tee", "The old ingest tees every event into Kinesis through the same PII "
     "scrubber. S3 starts filling.", "Turn the tee off."),
    ("Weeks 5 to 8", "Shadow", "Processor and ClickHouse run dark. Daily diff of distinct "
     "(tenant, event_id) per type and hour.", "Nothing is live yet."),
    ("Weeks 9 to 12", "MVP", "Real-time dashboards behind a per-tenant flag. Five design "
     "partners, then 10%, 50%, 100%.", "Flip the flag. The old pipeline never stopped."),
    ("Months 4 to 6", "Cutover", "The new collector takes the SDK hostname by weighted DNS. "
     "Warehouse export, segment triggers.", "DNS weight back to the old ingest."),
]


def run_tests() -> int:
    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"))
    result = unittest.TextTestRunner(stream=io.StringIO(), verbosity=0).run(suite)
    if not result.wasSuccessful():
        sys.exit(f"build refused: {len(result.failures) + len(result.errors)} test(s) failing")
    return result.testsRun


def pretty(raw: str, keys: set[str], whole: bool) -> str:
    """Pretty-print a redacted line and mark the lines a finding is about."""
    try:
        text = json.dumps(json.loads(raw), indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        return f'<mark class="mk mk-whole">{escape(raw)}</mark>'
    out = []
    for line in text.splitlines():
        key = line.strip().split('":', 1)[0].lstrip('"') if '":' in line else None
        hit = whole or (key in keys) or ("***" in line)
        out.append(f'<mark class="mk">{escape(line)}</mark>' if hit else escape(line))
    return "\n".join(out)


def render_rows(report) -> tuple[str, Counter]:
    by_line: dict[int, list] = {}
    for f in report.findings:
        by_line.setdefault(f.line, []).append(f)
    lines_per_action: Counter = Counter()
    rows = []
    for ev in report.events:
        finds = by_line.get(ev.line, [])
        actions = sorted({f.action for f in finds}, key=ACTION_ORDER.index) or ["clean"]
        for a in actions:
            lines_per_action[a] += 1
        keys = set().union(*(MARK_KEYS.get(f.anomaly, set()) for f in finds)) if finds else set()
        whole = any(f.anomaly in ("MALFORMED_JSON", "INVALID_ENCODING", "NOT_AN_OBJECT") for f in finds)
        seen_labels: dict[str, str] = {}
        for f in finds:
            seen_labels.setdefault(LABELS[f.anomaly], f.action)
        # The action rides along as hidden text so a screen reader hears what color says.
        marks = "".join(f'<span class="mark a-{a}">{escape(lbl)}<span class="vh">, {a}</span></span>'
                        for lbl, a in seen_labels.items()) \
            or '<span class="mark a-clean">clean</span>'
        detail = "".join(
            f'<div class="find a-{f.action}"><p class="find-head"><span class="find-cls">{escape(LABELS[f.anomaly])}</span>'
            f'<span class="find-act">{f.action}</span></p>'
            f'<p class="find-detail">{escape(f.detail)}</p><p class="find-how">{escape(f.handling)}</p></div>'
            for f in finds
        ) or '<div class="find a-clean"><p class="find-how">No findings. Passes straight through to the stores.</p></div>'
        meta = " · ".join(x for x in (ev.tenant_id or "no tenant", ev.type or "") if x)
        # Native disclosure: opens without JS, and find-in-page can open it.
        rows.append(f"""
<li class="row" id="L{ev.line}" data-actions="{' '.join(actions)}">
  <details>
    <summary class="row-head"><span class="row-grid">
      <span class="ln">{ev.line:02d}</span>
      <span class="eid">{escape(ev.event_id or 'no id')}</span>
      <span class="meta">{escape(meta)}</span>
      <span class="marks">{marks}</span>
    </span></summary>
    <div class="row-inner">
      <pre class="raw">{pretty(ev.raw, keys, whole)}</pre>
      <div class="finds">{detail}</div>
    </div>
  </details>
</li>""")
    return "".join(rows), lines_per_action


def spell(n: int) -> str:
    ones = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
            "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen",
            "eighteen", "nineteen"]
    tens = {2: "twenty", 3: "thirty", 4: "forty", 5: "fifty", 6: "sixty", 7: "seventy",
            8: "eighty", 9: "ninety"}
    if n < 20:
        word = ones[n]
    elif n < 100:
        word = tens[n // 10] + ("" if n % 10 == 0 else "-" + ones[n % 10])
    else:
        word = f"{n:,}"
    return word[:1].upper() + word[1:]


def usd(v: float) -> str:
    return f"${v:,.0f}"


def render_cost(m: dict) -> str:
    s = m["sizing"]
    budget = m["budget"]
    w = 600
    # Clamped, so a model change that blows the budget shows a full bar, not an overflow.
    new_w = min(m["new_total"] / budget, 1) * w
    old_w = min(m["old_pipeline"] / budget * w, w - new_w)
    over = m["during_migration"] > budget
    lines = "".join(
        f'<tr><td>{escape(l["name"])}</td><td class="num">{usd(l["usd"])}</td></tr>' for l in m["lines"])
    return f"""
<div class="calc">
  <dl class="sizing">
    <div><dt>average</dt><dd>{s['avg_eps']:,.0f}<small>/s</small></dd><dd class="tag">estimated</dd></div>
    <div><dt>design peak</dt><dd>{s['design_eps']:,.0f}<small>/s</small></dd><dd class="tag">estimated · 10x spike, 1.5x headroom assumed</dd></div>
    <div><dt>shards</dt><dd>{s['shards']}</dd><dd class="tag">estimated · 1,000 records/s each, AWS limit</dd></div>
    <div><dt>if the spike lands on the busiest hour</dt><dd>{s['peak_shards']}</dd><dd class="tag">estimated · +{usd(m['peak_shard_delta_usd'])}/mo, 3x peak hour assumed</dd></div>
  </dl>
  <figure class="meter{' over' if over else ''}">
    <div class="meter-bar">
      <svg viewBox="0 0 {w} 28" preserveAspectRatio="none" role="img"
           aria-label="New pipeline {usd(m['new_total'])} plus old pipeline {usd(m['old_pipeline'])} against a {usd(budget)} monthly ceiling">
        <defs><pattern id="hatch" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
          <line x1="0" y1="0" x2="0" y2="6" class="hatch-line"/></pattern></defs>
        <rect class="m-new" x="0" y="0" width="{new_w:.1f}" height="28"/>
        <rect class="m-old" x="{new_w:.1f}" y="0" width="{old_w:.1f}" height="28" fill="url(#hatch)"/>
      </svg>
    </div>
    <figcaption>
      <span><i class="k k-new"></i>new pipeline {usd(m['new_total'])}</span>
      <span><i class="k k-old"></i>old one, still running during the parallel run {usd(m['old_pipeline'])} <em>assumed</em></span>
      <span class="right">{'over the ' if over else ''}ceiling {usd(budget)}</span>
    </figcaption>
  </figure>
  <table class="costs">
    <caption>Monthly. AWS unit prices checked against AWS pricing pages on Oct 22, 2026. ClickHouse, transfer and observability are <em>estimates</em>, not quotes.</caption>
    <tbody>{lines}</tbody>
    <tfoot>
      <tr><td>New pipeline</td><td class="num">{usd(m['new_total'])}</td></tr>
      <tr><td>Headroom while both run</td><td class="num">{usd(m['headroom'])}</td></tr>
    </tfoot>
  </table>
</div>"""


def packet_is_final() -> bool:
    """The PDF is only published once the AI disclosure TODO is gone."""
    return PDF.exists() and "TODO DAVID" not in (ROOT / "SUBMISSION.md").read_text(encoding="utf-8")


def render_page(report, cost: dict, tests: int, pdf: bool = False) -> str:
    rows, per_action = render_rows(report)
    total_lines = len(report.events)
    chips = [f'<button class="chip" type="button" data-filter="all" aria-pressed="true">all <b>{total_lines}</b></button>']
    for a in [*ACTION_ORDER, "clean"]:
        if per_action.get(a):
            chips.append(f'<button class="chip a-{a}" type="button" data-filter="{a}" aria-pressed="false">'
                         f'{a} <b>{per_action[a]}</b></button>')
    legend = "".join(f"<li><span class='mark a-{a}'>{a}</span> {escape(ACTION_NOTES[a])}</li>"
                     for a in ACTION_ORDER if per_action.get(a))
    face = "".join(
        (f'<li><a class="fv-id" href="#L{line}">{escape(eid)}</a>' if line
         else f'<li><span class="fv-id">{escape(eid)}</span>')
        + f'<p><strong>{title}</strong> {escape(body)}</p></li>'
        for eid, line, title, body in FACE_VALUE)
    phases = "".join(
        f'<li><p class="ph-when">{escape(w)}</p><h3>{escape(n)}</h3><p>{escape(d)}</p>'
        f'<p class="ph-back"><span>rollback</span> {escape(b)}</p></li>' for w, n, d, b in PHASES)
    matches = report.summary["fixture_matches"]
    lines_word = spell(len(report.events))
    clean_word = spell(len(report.clean_event_ids))

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>Engineer 004 · David Davieson</title>
<meta name="description" content="{DESCRIPTION}">
<link rel="canonical" href="{SITE}/">
<meta property="og:type" content="website">
<meta property="og:url" content="{SITE}/">
<meta property="og:title" content="Engineer 004 · David Davieson">
<meta property="og:description" content="{DESCRIPTION}">
<meta property="og:image" content="{SITE}/og.png">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
<meta name="theme-color" content="#050505">
<meta name="color-scheme" content="dark">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&display=swap">
<link rel="stylesheet" href="/styles.css">
<script src="/app.js" defer></script>
</head>
<body>
<a class="skip" href="#ledger">Skip to the fixture</a>
<header class="top glass">
  <a class="brand" href="#top"><span class="brand-mark" aria-hidden="true"></span>Engineer 004</a>
  <nav aria-label="Sections">
    <a href="#ledger">fixture</a><a href="#face">face value</a><a href="#shape">pipeline</a><a href="#math">math</a><a href="#cutover">cutover</a>
  </nav>
  <a class="out" href="{REPO}">repo <span aria-hidden="true">↗</span></a>
</header>

<main id="top" class="wrap">
  <section class="hero">
    <p class="kicker">Beat Claude · Engineer 004 · brief version 2026-07</p>
    <h1>{lines_word} lines.<br><span>{clean_word} clean events.</span></h1>
    <p class="lede">The brief wants a pipeline that takes 50 million events a day without losing any.
      This is the sample it has to survive first: every line of the challenge fixture, what's wrong
      with it, and what the pipeline does about it.</p>
    <ul class="proof">
      <li class="{'ok' if matches else 'bad'}"><span>fixture</span> sha256 {report.sha256_lf[:12]}… {'matches the published file' if matches else 'DOES NOT MATCH'}</li>
      <li class="ok"><span>tests</span> {tests} passing at build</li>
      <li><span>detector</span> v{__version__}, stdlib python</li>
    </ul>
  </section>

  <section id="ledger" class="ledger-wrap" aria-labelledby="ledger-h">
    <div class="sec-head">
      <h2 id="ledger-h">The fixture, line by line</h2>
      <p>Open a line for the raw event and the handling. Filters dim lines instead of hiding them, so the file stays the file.</p>
    </div>
    <div class="filters glass" role="group" aria-label="Filter by what the pipeline does">{''.join(chips)}</div>
    <p class="vh" id="status" role="status"></p>
    <ul class="legend">{legend}</ul>
    <ol class="ledger glass" data-filter="all">{rows}</ol>
  </section>

  <section id="face" aria-labelledby="face-h">
    <div class="sec-head">
      <h2 id="face-h">What I won't take at face value</h2>
      <p>The planted issues are the easy part. These are the conclusions the data seems to offer that I'd refuse.</p>
    </div>
    <ul class="face">{face}</ul>
  </section>

  <section id="shape" aria-labelledby="shape-h">
    <div class="sec-head">
      <h2 id="shape-h">The pipeline</h2>
      <p>Built for two engineers in three months. Every box below is something the team already runs, or a managed service.</p>
    </div>
    <div class="flow">
      <div class="lane"><p class="lane-h">ingest</p>
        <div class="node glass"><h3>JS SDK</h3><p>unchanged. The fixture suggests it retries (evt-0002 arrives twice).</p></div>
        <div class="node glass"><h3>Collector on ECS</h3><p>stamps received_at, strips PII, acks only after Kinesis does</p></div>
      </div>
      <div class="lane"><p class="lane-h">stream</p>
        <div class="node glass key"><h3>Kinesis</h3><p>7 days of replay. Partition key tenant:anon. Chosen over MSK: no brokers for two people to run.</p></div>
        <div class="node glass"><h3>Lambda processor</h3><p>this repo's detect.py, in production form. Dedupe, normalize, quarantine.</p></div>
      </div>
      <div class="lane"><p class="lane-h">serve</p>
        <div class="node glass"><h3>S3 Parquet</h3><p>system of record. Replay, backfill, warehouse export.</p></div>
        <div class="node glass"><h3>Redis</h3><p>personalization hot path, about 2s from click (estimated). Dedupe keys live 1 hour.</p></div>
        <div class="node glass"><h3>ClickHouse</h3><p>dashboards and segments. ReplacingMergeTree on (tenant, event_id) backstops dedupe.</p></div>
      </div>
    </div>
    <p class="note">Deletion never rides the event stream. It comes in through an authenticated tenant API, walks the identity graph, and reaches S3, ClickHouse and Redis inside GDPR's one month.</p>
  </section>

  <section id="math" aria-labelledby="math-h">
    <div class="sec-head">
      <h2 id="math-h">The arithmetic</h2>
      <p>From <code>cost_model.py</code>. Change an input, rebuild, and this section moves with it.</p>
    </div>
    {render_cost(cost)}
  </section>

  <section id="cutover" aria-labelledby="cutover-h">
    <div class="sec-head">
      <h2 id="cutover-h">Cutover</h2>
      <p>No SDK change, no broken integration. Every step has a way back that doesn't need a deploy.</p>
    </div>
    <ol class="phases">{phases}</ol>
    <p class="note">Verified means distinct (tenant_id, event_id) at the raw layer, both sides, for 14 days. Never dashboard totals: the old system may count retries twice.</p>
  </section>

  <section class="run" aria-labelledby="run-h">
    <h2 id="run-h">Run it yourself</h2>
    <div class="cmd glass">
      <pre id="cmds">git clone {REPO}
cd beat-claude-engineer-004
python detect.py fixtures/event_sample.jsonl
python -m unittest discover -s tests -v
python cost_model.py</pre>
      <button class="copy" type="button" data-copy="cmds"><span class="copy-a">copy</span><span class="copy-b" aria-hidden="true">copied</span></button>
    </div>
    <p class="links"><a href="{REPO}/blob/main/SUBMISSION.md">written packet</a>{' · <a href="/SUBMISSION.pdf">packet as PDF</a>' if pdf else ''} · <a href="/report.json">report.json</a> · <a href="/cost.json">cost.json</a> · <a href="{CHALLENGE}">the brief</a></p>
  </section>
</main>
<footer class="wrap foot">
  <p>David Davieson · <a href="https://nuroctane.xyz">nuroctane.xyz</a> · built with an AI coding agent, disclosed in the packet</p>
</footer>
</body>
</html>
"""


def main() -> None:
    tests = run_tests()
    report = analyze(FIXTURE)
    # The face-value prose cites specific lines and counts from this fixture.
    # A rotated fixture would make it quietly wrong, so refuse instead.
    if not report.summary["fixture_matches"] and "--allow-fixture-mismatch" not in sys.argv:
        sys.exit(f"build refused: fixture is not {EXPECTED_SHA256[:12]}; the page's prose "
                 "would be stale. Re-answer the new fixture, or pass --allow-fixture-mismatch.")
    cost = cost_model.compute()

    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    for f in SRC.iterdir():
        shutil.copy2(f, OUT / f.name)
    pdf = packet_is_final()
    if pdf:
        shutil.copy2(PDF, OUT / PDF.name)
    (OUT / "index.html").write_text(render_page(report, cost, tests, pdf=pdf), encoding="utf-8")
    (OUT / "report.json").write_text(json.dumps(asdict(report), indent=2) + "\n", encoding="utf-8")
    (OUT / "cost.json").write_text(json.dumps(cost, indent=2) + "\n", encoding="utf-8")
    (OUT / "404.html").write_text(
        '<!doctype html><html lang="en"><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        '<title>Not here · Engineer 004</title><link rel="icon" href="/favicon.svg" type="image/svg+xml">'
        '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&display=swap">'
        '<link rel="stylesheet" href="/styles.css">'
        '<main class="wrap hero"><p class="kicker">404</p><h1>Not in the fixture.</h1>'
        '<p class="lede"><a href="/">Back to the ledger</a></p></main>', encoding="utf-8")
    print(f"built {OUT} ({tests} tests passing, {len(report.events)} lines, {len(report.findings)} findings)")


if __name__ == "__main__":
    main()
