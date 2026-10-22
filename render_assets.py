"""Render the upload assets: SUBMISSION.pdf and the link-preview card site/src/og.png.

Dev tool, not part of the artifact. Needs markdown and Playwright's Chromium:

    uv run --with playwright --with markdown python render_assets.py

Mermaid and the web fonts load from CDNs, so it needs network access.
While the AI disclosure still has its TODO, the PDF is stamped DRAFT and the
site won't publish it.
"""

from __future__ import annotations

import os
import re
from html import escape
from pathlib import Path

import markdown
from playwright.sync_api import sync_playwright

from build_site import spell
from detect import analyze

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "SUBMISSION.md"
OUT = ROOT / "SUBMISSION.pdf"
OG = ROOT / "site" / "src" / "og.png"
TODO = "TODO DAVID"

CSS = """
@page { size: Letter; margin: 0.6in 0.65in; }
:root { --ink: #141414; --muted: #555; --rule: #d9d9d9; }
* { box-sizing: border-box; }
body { margin: 0; color: var(--ink); font: 9.4pt/1.42 'IBM Plex Sans', system-ui, sans-serif; }
h1 { margin: 0 0 6pt; font: 600 15pt/1.15 'JetBrains Mono', monospace; letter-spacing: -.02em; }
h2 { margin: 14pt 0 5pt; padding-top: 6pt; border-top: 1px solid var(--rule);
     font: 600 11pt/1.2 'JetBrains Mono', monospace; break-after: avoid; }
h3 { margin: 10pt 0 3pt; font: 600 9.6pt/1.2 'JetBrains Mono', monospace; break-after: avoid; }
p { margin: 0 0 5pt; }
ul, ol { margin: 0 0 6pt; padding-left: 14pt; }
li { margin: 0 0 2pt; }
strong { font-weight: 600; }
code { font: 8.2pt 'JetBrains Mono', monospace; background: #f2f2f2; padding: 0 2pt; border-radius: 2px; }
pre { margin: 0 0 6pt; padding: 6pt 8pt; background: #f6f6f6; border-radius: 4px; break-inside: avoid; }
pre code { background: none; padding: 0; }
table { width: 100%; margin: 0 0 7pt; border-collapse: collapse; font-size: 8.3pt; line-height: 1.32; }
th, td { padding: 3pt 6pt 3pt 0; border-bottom: 1px solid var(--rule); text-align: left; vertical-align: top; }
th { font: 600 7.4pt 'JetBrains Mono', monospace; text-transform: uppercase; letter-spacing: .04em; color: var(--muted); }
tr { break-inside: avoid; }
hr { border: 0; margin: 6pt 0; }
.mermaid { margin: 4pt 0 8pt; text-align: center; break-inside: avoid; }
.mermaid svg { max-width: 100%; max-height: 3.2in; }
.packet-start { break-before: page; border-top: 0; padding-top: 0; margin-top: 0; }
.nw { white-space: nowrap; }
.keep { break-inside: avoid; }
p:has(+ table), p:has(+ ol), p:has(+ ul) { break-after: avoid; }
.draft { margin: 0 0 8pt; padding: 5pt 8pt; border: 1.5px solid #b3261e; color: #b3261e;
         font: 600 8.5pt 'JetBrains Mono', monospace; }
"""


def to_html(md_text: str) -> str:
    diagrams: list[str] = []

    def stash(m):
        diagrams.append(m.group(1))
        return f"\n\nMERMAID{len(diagrams) - 1}\n\n"

    # Pull Mermaid out before Markdown escapes it.
    md_text = re.sub(r"```mermaid\n(.*?)```", stash, md_text, flags=re.S)
    body = markdown.markdown(md_text, extensions=["tables", "fenced_code", "sane_lists"])
    for i, d in enumerate(diagrams):
        body = body.replace(f"<p>MERMAID{i}</p>", f'<div class="mermaid">{escape(d)}</div>')
    # The written answer is the part under the page cap; the packet starts fresh.
    body = body.replace("<h2>2. Operating artifact", '<h2 class="packet-start">2. Operating artifact')
    body = re.sub(r"<td>(Tier \d)</td>", r'<td class="nw">\1</td>', body)
    # The last section is short; keep it whole so its heading never strands a page early.
    body = body.replace("<h2>8. Artifact access</h2>", '<section class="keep"><h2>8. Artifact access</h2>', 1)
    if '<section class="keep">' in body:
        body += "</section>"
    banner = ('<div class="draft">DRAFT: the AI usage disclosure still has a TODO. '
              'Fill it in, then re-run render_pdf.py.</div>') if TODO in md_text else ""
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>Beat Claude Engineer 004 - David Davieson</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;600&family=JetBrains+Mono:wght@400;600&display=swap">
<style>{CSS}</style>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.min.js"></script>
</head><body>{banner}{body}</body></html>"""


def installed_chromium() -> str | None:
    """Newest Chromium already in the Playwright cache, so a version mismatch
    between the pip package and the cache doesn't force a fresh download."""
    cache = Path(os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
                 or Path(os.environ.get("LOCALAPPDATA", Path.home())) / "ms-playwright")
    found = sorted(cache.glob("chromium-*/chrome-*/chrome*"),
                   key=lambda p: int(re.sub(r"\D", "", p.parts[-3]) or 0))
    found = [p for p in found if p.name in ("chrome.exe", "chrome", "Chromium")]
    return str(found[-1]) if found else None


def card(lines: str, clean: str) -> str:
    rows = [("05", "evt-0002", "duplicate retry", ""), ("08", "evt-0007", "pii", "gold"),
            ("12", "evt-0011", "no tenant", "gold"), ("18", "evt-0017", "deletion via sdk", "gold"),
            ("21", "evt-0020", "malformed json", "gold")]
    ledger = "".join(
        f'<div class="r"><span class="ln">{ln}</span><span class="id">{eid}</span>'
        f'<span class="m {cls}">{lbl}</span></div>' for ln, eid, lbl, cls in rows)
    return f"""<!doctype html><html><head><meta charset="utf-8">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&display=swap">
<style>
* {{ box-sizing: border-box; margin: 0; }}
body {{ width: 1200px; height: 630px; padding: 72px 80px; background: #050505; color: #f2f2f2;
       font-family: 'JetBrains Mono', monospace; position: relative; overflow: hidden;
       background-image: radial-gradient(56rem 34rem at 82% -10%, rgb(255 255 255 / 8%), transparent 62%),
         radial-gradient(circle at 1px 1px, rgb(255 255 255 / 8%) 1px, transparent 0);
       background-size: auto, 22px 22px; }}
.k {{ color: #a0a0a0; font-size: 18px; letter-spacing: .14em; text-transform: uppercase; }}
h1 {{ margin-top: 34px; font-size: 76px; font-weight: 500; line-height: 1.02; letter-spacing: -.045em; }}
h1 span {{ color: #8a8a8a; }}
.led {{ position: absolute; right: 80px; bottom: 72px; width: 430px; padding: 8px;
        border: 1px solid rgb(255 255 255 / 12%); border-radius: 14px;
        background: linear-gradient(145deg, rgb(255 255 255 / 6%), transparent 65%), rgb(20 20 20 / 80%);
        box-shadow: inset 0 1px 0 rgb(255 255 255 / 9%), 0 18px 55px rgb(0 0 0 / 40%); }}
.r {{ display: grid; grid-template-columns: 34px 1fr auto; gap: 14px; align-items: center;
      padding: 10px 10px; font-size: 18px; }}
.r + .r {{ border-top: 1px solid rgb(255 255 255 / 6%); }}
.ln {{ color: #8a8a8a; text-align: right; }}
.m {{ padding: 2px 9px; border: 1px solid rgb(255 255 255 / 28%); border-radius: 7px; font-size: 15px; }}
.m.gold {{ color: #e8d49a; border-color: rgb(232 212 154 / 55%); background: rgb(232 212 154 / 10%); }}
.by {{ position: absolute; left: 80px; bottom: 72px; max-width: 520px; color: #a0a0a0; font-size: 20px; line-height: 1.5; }}
.by b {{ color: #f2f2f2; font-weight: 500; }}
</style></head><body>
<p class="k">Beat Claude · Engineer 004</p>
<h1>{lines} lines.<br><span>{clean} clean events.</span></h1>
<p class="by"><b>David Davieson</b><br>a real-time pipeline, argued from the data</p>
<div class="led">{ledger}</div>
</body></html>"""


def render_pdf(browser) -> int:
    md_text = SRC.read_text(encoding="utf-8")
    page = browser.new_page()
    page.set_content(to_html(md_text), wait_until="networkidle")
    page.evaluate("async () => { mermaid.initialize({ startOnLoad: false, theme: 'neutral' });"
                  " await mermaid.run(); await document.fonts.ready; }")
    page.pdf(path=str(OUT), format="Letter", print_background=True,
             margin={"top": "0.6in", "bottom": "0.6in", "left": "0.65in", "right": "0.65in"})
    page.close()
    pages = len(re.findall(rb"/Type\s*/Page(?![s\w])", OUT.read_bytes()))
    print(f"wrote {OUT.name}: {pages} page(s)" + ("  [DRAFT: AI disclosure TODO]" if TODO in md_text else ""))
    return pages


def render_og(browser) -> None:
    report = analyze(ROOT / "fixtures" / "event_sample.jsonl")
    page = browser.new_page(viewport={"width": 1200, "height": 630})
    page.set_content(card(spell(len(report.events)), spell(len(report.clean_event_ids))),
                     wait_until="networkidle")
    page.evaluate("document.fonts.ready")
    page.screenshot(path=str(OG))
    page.close()
    print(f"wrote {OG.relative_to(ROOT)}")


def main() -> None:
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception:
            browser = p.chromium.launch(executable_path=installed_chromium())
        render_pdf(browser)
        render_og(browser)
        browser.close()


if __name__ == "__main__":
    main()
