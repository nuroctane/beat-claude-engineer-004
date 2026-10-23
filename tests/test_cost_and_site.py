"""The cost model's arithmetic and the report page's contract."""

import json
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import cost_model  # noqa: E402
from build_site import render_page, spell  # noqa: E402
from detect import analyze  # noqa: E402


class CostModel(unittest.TestCase):
    def setUp(self):
        self.m = cost_model.compute()

    def test_shards_cover_records_and_bytes(self):
        s = self.m["sizing"]
        self.assertGreaterEqual(s["shards"] * cost_model.SHARD_RECORDS_PER_S, s["design_eps"])
        self.assertGreaterEqual(s["shards"] * cost_model.SHARD_BYTES_PER_S,
                                s["design_eps"] * cost_model.EVENT_BYTES)
        self.assertEqual((s["shards"], s["peak_shards"]), (9, 27))

    def test_dedupe_window_fits_one_redis_node_and_24h_would_not(self):
        s = self.m["sizing"]
        self.assertLess(s["dedupe_gb"], s["redis_node_gb"])
        self.assertGreater(s["dedupe_gb_24h_peak_day"], s["redis_node_gb"])

    def test_firehose_bills_small_records_as_5kb(self):
        s = self.m["sizing"]
        self.assertAlmostEqual(s["firehose_billed_gb"] / s["raw_gb_month"], 5.12, places=2)

    def test_totals_add_up_and_fit_the_ceiling(self):
        self.assertAlmostEqual(sum(l["usd"] for l in self.m["lines"]), self.m["new_total"], delta=0.1)
        self.assertLess(self.m["during_migration"], self.m["budget"])
        self.assertTrue(all(l["usd"] > 0 for l in self.m["lines"]))


class Page(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        report = analyze(ROOT / "fixtures" / "event_sample.jsonl")
        cls.html = render_page(report, cost_model.compute(), tests=0)

    def test_every_fixture_line_is_a_row(self):
        self.assertEqual(len(re.findall(r'<li class="row" id="L\d+"', self.html)), 25)

    def test_no_pii_on_the_page(self):
        self.assertNotIn("maria.gonzalez", self.html)
        self.assertNotIn("555-0142", self.html)

    def test_face_value_links_point_at_real_rows(self):
        for target in re.findall(r'class="fv-id" href="#(L\d+)"', self.html):
            self.assertIn(f'id="{target}"', self.html)

    def test_hero_counts_come_from_the_report(self):
        self.assertIn("Twenty-five lines.", self.html)
        self.assertIn("Eleven clean events.", self.html)

    def test_load_test_section_comes_from_results(self):
        results = ROOT / "bench" / "results.json"
        if not results.exists():
            self.skipTest("benchmark hasn't run")
        r = json.loads(results.read_text(encoding="utf-8"))
        html = render_page(analyze(ROOT / "fixtures" / "event_sample.jsonl"), cost_model.compute(),
                           tests=0, bench=r)
        self.assertIn('id="bench"', html)
        self.assertIn(f"{r['stream']['rows_per_s']:,}", html)

    def test_spell(self):
        self.assertEqual([spell(n) for n in (0, 11, 20, 25, 99, 1200)],
                         ["Zero", "Eleven", "Twenty", "Twenty-five", "Ninety-nine", "1,200"])


if __name__ == "__main__":
    unittest.main()
