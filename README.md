# beat-claude-engineer-004

My answer to Single Grain's [Beat Claude](https://github.com/ericosiu/beat-claude) Engineer 004 brief (version 2026-07): a real-time analytics pipeline on AWS, argued line by line from the challenge fixture.

- [`SUBMISSION.md`](SUBMISSION.md): the written answer and full packet
- `detect.py`: the pipeline's validation stage, run against the fixture
- `tests/`: 54 tests: every finding pinned to an event_id, the edge cases a real stream throws, the cost math, and the page
- `cost_model.py`: sizing and monthly cost, every input labeled
- `build_site.py` + `site/src/`: the interactive report, rendered from the same code

```
python detect.py fixtures/event_sample.jsonl
python -m unittest discover -s tests -v
python cost_model.py
python build_site.py          # writes site/public, refuses to build if a test fails
```

Useful flags on `detect.py`: `--json out.json`, `--fail-on quarantine,compliance` (exit 1 for CI), `--check-upstream` (exit 2 if the live challenge fixture has rotated), `-` to read stdin.

Stdlib Python 3.10+. `fixtures/event_sample.jsonl` is a byte-exact copy of the public fixture (commit 4b719e4), pinned by `.gitattributes` so Windows checkouts keep its LF line endings and its sha256.

Upload assets: `uv run --with playwright --with markdown python render_assets.py` writes `SUBMISSION.pdf` (pages 1 to 4 are the written answer) and the link-preview card. The PDF is stamped DRAFT, and the site won't publish it, until the AI disclosure TODO is filled in.

Deploy the report: `python build_site.py && npx wrangler deploy`.
