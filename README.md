# beat-claude-engineer-004

My answer to the Engineer 004 brief (version 2026-07) in Single Grain's [Beat Claude](https://github.com/ericosiu/beat-claude) challenge. It's a real-time analytics pipeline on AWS, argued from the challenge's 25-line fixture.

Live report: https://beat-claude-engineer-004.civetadeicollegium.workers.dev

## What's here

- [`SUBMISSION.md`](SUBMISSION.md): the written answer and the required packet. [`SUBMISSION.pdf`](SUBMISSION.pdf) is the same thing for upload.
- `detect.py`: the pipeline's validation stage, run against the fixture. Every finding names an `event_id` and what the pipeline does with it.
- `tests/`: 55 tests. They pin every finding, cover the malformed input a real stream sends, and check the cost math and the report page.
- `cost_model.py`: sizing and monthly cost. Every input carries a source label, and the AWS prices link to their pricing pages.
- `bench/`: a ClickHouse benchmark at one day of the brief's volume. Results are in [`bench/RESULTS.md`](bench/RESULTS.md).
- `build_site.py` and `site/src/`: the report page, rendered from the same detector and cost model output.

## Run it

Stdlib Python 3.10 or newer. No installs, no keys.

```bash
python detect.py fixtures/event_sample.jsonl
python -m unittest discover -s tests -v
python cost_model.py
```

`detect.py` flags:

- `--json out.json` writes the full report.
- `--fail-on quarantine,compliance` exits 1 when those actions show up, for CI.
- `--check-upstream` exits 2 if the live challenge fixture has rotated.
- `-` reads events from stdin.

The fixture is a byte-exact copy of the public one (commit `4b719e4`). `.gitattributes` keeps Windows checkouts from rewriting its line endings, so the sha256 stays `1aeb24b4…`.

## Rebuild the assets

The benchmark needs Docker:

```bash
python bench/run_bench.py
```

The PDF and the link-preview card need Playwright's Chromium:

```bash
uv run --with playwright --with markdown python render_assets.py
```

The report page refuses to build if a test fails. Deploy it to Cloudflare Workers:

```bash
python build_site.py && npx wrangler deploy
```
