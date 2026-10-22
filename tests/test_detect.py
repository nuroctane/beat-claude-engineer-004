"""Run: python -m unittest discover -s tests -v"""

import io
import json
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from detect import ACTIONS, EXPECTED_SHA256, analyze, analyze_bytes, main  # noqa: E402

FIXTURE = ROOT / "fixtures" / "event_sample.jsonl"


def by_class(report):
    out = {}
    for f in report.findings:
        out.setdefault(f.anomaly, set()).add(f.event_id)
    return out


class FixtureFindings(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report = analyze(FIXTURE)
        cls.found = by_class(cls.report)

    def test_checksum_pins_the_published_fixture(self):
        self.assertEqual(self.report.sha256, EXPECTED_SHA256)
        self.assertTrue(self.report.summary["fixture_matches"])

    def test_counts(self):
        self.assertEqual(self.report.lines, 25)
        self.assertEqual(self.report.parsed, 24)
        self.assertEqual(self.report.unique_events, 23)

    def test_each_seeded_issue_is_caught(self):
        expected = {
            "DUPLICATE_RETRY": {"evt-0002"},
            "CLOCK_SKEW_FUTURE": {"evt-0005", "evt-0006"},
            "PII_IN_PAYLOAD": {"evt-0007"},
            "LEGACY_SCHEMA": {"evt-0009"},
            "MISSING_RECEIVED_AT": {"evt-0009"},
            "MISSING_TENANT": {"evt-0011"},
            "BOT_SUSPECTED": {"evt-0012", "evt-0013", "evt-0014", "evt-0015"},
            "TIMESTAMP_IMPLAUSIBLE": {"evt-0016"},
            "PRIVACY_REQUEST_VIA_SDK": {"evt-0017"},
            "DELETION_SCOPE": {"evt-0017"},
            "CLIENT_COMPUTED_METRIC": {"evt-0019"},
            "CLIENT_COUNTER_MISMATCH": {"evt-0019"},
            "MALFORMED_JSON": {"evt-0020"},
        }
        self.assertEqual(self.found, expected)

    def test_no_false_positives_on_clean_events(self):
        # evt-0002's first copy is clean; only the retry on line 5 is flagged.
        self.assertEqual(
            self.report.clean_event_ids,
            ["evt-0001", "evt-0002", "evt-0003", "evt-0004", "evt-0008", "evt-0010",
             "evt-0018", "evt-0021", "evt-0022", "evt-0023", "evt-0024"],
        )

    def test_duplicate_is_flagged_on_the_retry_line_only(self):
        dup = next(f for f in self.report.findings if f.anomaly == "DUPLICATE_RETRY")
        self.assertEqual(dup.line, 5)

    def test_identity_stitching_is_tenant_scoped(self):
        g = self.report.identity_graph
        self.assertEqual(g["t-042/u-5511"], ["anon-9f2"])
        self.assertEqual(g["t-042/u-7304"], ["anon-3d0"])
        self.assertEqual(g["t-088/u-1077"], ["anon-77a"])

    def test_deletion_reaches_pre_identify_history(self):
        scope = next(f for f in self.report.findings if f.anomaly == "DELETION_SCOPE")
        self.assertIn("evt-0006", scope.detail)

    def test_pii_never_leaks_into_any_output(self):
        blob = json.dumps(self.report, default=lambda o: o.__dict__)
        self.assertNotIn("maria.gonzalez", blob)
        self.assertNotIn("555-0142", blob)

    def test_every_finding_has_an_action_and_handling(self):
        for f in self.report.findings:
            self.assertEqual(f.action, ACTIONS[f.anomaly])
            self.assertTrue(f.handling)

    def test_crlf_copy_still_matches(self):
        crlf = analyze_bytes(FIXTURE.read_bytes().replace(b"\n", b"\r\n"))
        self.assertNotEqual(crlf.sha256, EXPECTED_SHA256)
        self.assertTrue(crlf.summary["fixture_matches"])
        self.assertEqual(by_class(crlf), self.found)


class SyntheticCases(unittest.TestCase):
    def run_lines(self, *records):
        body = "\n".join(r if isinstance(r, str) else json.dumps(r) for r in records) + "\n"
        return analyze_bytes(body.encode("utf-8"))

    def found(self, *records):
        return by_class(self.run_lines(*records))

    def base(self, **kw):
        ev = {"event_id": "e1", "tenant_id": "t-1", "anonymous_id": "a1", "user_id": None,
              "type": "page_view", "ts": "2026-06-15T14:00:00.000Z",
              "received_at": "2026-06-15T14:00:00.200Z", "properties": {"path": "/"}}
        ev.update(kw)
        return ev

    # Dedupe and identity
    def test_same_id_different_payload_is_not_deduped(self):
        self.assertEqual(self.found(self.base(), self.base(properties={"path": "/other"})),
                         {"EVENT_ID_COLLISION": {"e1"}})

    def test_same_id_across_tenants_is_two_events(self):
        self.assertEqual(self.found(self.base(), self.base(tenant_id="t-2")), {})

    def test_missing_event_ids_are_not_deduped_against_each_other(self):
        r = self.run_lines(self.base(event_id=None), self.base(event_id="", anonymous_id="a2"))
        self.assertEqual(by_class(r), {"MISSING_EVENT_ID": {None}})
        self.assertEqual(r.unique_events, 2)

    def test_anonymous_id_identified_as_two_users(self):
        found = self.found(self.base(type="identify", user_id="u1"),
                           self.base(event_id="e2", type="identify", user_id="u2"))
        self.assertEqual(found, {"ANON_MULTI_USER": {"e2"}})

    # Timestamps
    def test_late_arrival(self):
        self.assertEqual(self.found(self.base(ts="2026-06-13T14:00:00.000Z")), {"LATE_ARRIVAL": {"e1"}})

    def test_small_skew_within_tolerance_is_clean(self):
        self.assertEqual(self.found(self.base(ts="2026-06-15T14:00:03.000Z")), {})

    def test_years_old_timestamp_is_a_clock_problem_not_a_late_event(self):
        r = self.run_lines(self.base(ts="2021-06-15T14:00:00.000Z"))
        self.assertEqual(by_class(r), {"TIMESTAMP_IMPLAUSIBLE": {"e1"}})
        self.assertIn("1826 days before receipt", r.findings[0].detail)

    def test_iso_shapes_parse_the_same_on_every_python(self):
        for ts in ("2026-06-15T14:00:00.1Z", "2026-06-15T14:00:00.123456789Z",
                   "2026-06-15T14:00:00.000+0000", "2026-06-15T14:00:00z"):
            self.assertEqual(self.found(self.base(ts=ts)), {}, ts)

    def test_naive_timestamp_does_not_crash(self):
        self.assertEqual(self.found(self.base(ts="2026-06-15T14:00:00")), {"TIMESTAMP_NO_TZ": {"e1"}})

    def test_epoch_millis_and_seconds_are_accepted(self):
        self.assertEqual(self.found(self.base(ts=1781532000000)), {})
        self.assertEqual(self.found(self.base(ts=1781532000)), {})

    def test_garbage_timestamp(self):
        self.assertEqual(self.found(self.base(ts="yesterday")), {"TIMESTAMP_UNPARSEABLE": {"e1"}})
        self.assertEqual(self.found(self.base(ts=True)), {"TIMESTAMP_UNPARSEABLE": {"e1"}})

    # Shapes
    def test_non_object_json_is_quarantined(self):
        self.assertEqual(self.found("[1, 2, 3]", '"hello"', "null"), {"NOT_AN_OBJECT": {None}})

    def test_properties_not_an_object(self):
        self.assertEqual(self.found(self.base(properties="oops")), {"PROPERTIES_NOT_OBJECT": {"e1"}})

    def test_invalid_utf8_is_quarantined_not_fatal(self):
        r = analyze_bytes(b'{"event_id":"e1"\xff}\n' + json.dumps(self.base(event_id="e2")).encode())
        self.assertEqual(by_class(r), {"INVALID_ENCODING": {None}})
        self.assertEqual(r.clean_event_ids, ["e2"])

    def test_bom_on_first_line(self):
        r = analyze_bytes(b"\xef\xbb\xbf" + json.dumps(self.base()).encode())
        self.assertEqual(by_class(r), {})

    def test_unknown_type(self):
        self.assertEqual(self.found(self.base(type="video_play")), {"UNKNOWN_EVENT_TYPE": {"e1"}})

    # PII: catch the real thing, leave dates and ids alone
    def test_dates_and_ids_are_not_pii(self):
        props = {"path": "/", "signup_date": "2026-06-15", "order_id": "1234567890",
                 "email_opt_in": True, "total_price": 49}
        self.assertEqual(self.found(self.base(properties=props)), {})

    def test_nested_pii_and_pii_in_identifiers(self):
        found = self.found(self.base(properties={"path": "/", "form": {"fields": [{"v": "(512) 555-0100"}]}}),
                           self.base(event_id="e2", user_id="bob@corp.example"))
        self.assertEqual(found, {"PII_IN_PAYLOAD": {"e1", "e2"}})

    def test_key_hints_match_the_last_word_only(self):
        props = {"path": "/", "adobe_campaign": "summer", "iphone_model": "15 Pro",
                 "email_campaign_id": "spring-26", "ip_address": "203.0.113.9"}
        r = self.run_lines(self.base(properties=props))
        self.assertEqual(by_class(r), {"PII_IN_PAYLOAD": {"e1"}})
        self.assertIn("ip_address", r.findings[0].detail)
        self.assertNotIn("adobe", r.findings[0].detail)

    def test_key_hinted_pii_is_masked_everywhere(self):
        r = self.run_lines(self.base(properties={"path": "/", "firstName": "Maria",
                                                 "billing": {"street": "12 Elm St"}}))
        blob = json.dumps(r, default=lambda o: o.__dict__)
        self.assertNotIn("Maria", blob)
        self.assertNotIn("Elm", blob)
        self.assertIn("M***", r.events[0].raw)

    def test_email_as_user_id_never_leaks_into_any_output(self):
        r = self.run_lines(self.base(type="identify", user_id="bob@corp.example"),
                           self.base(event_id="p", type="privacy_request", user_id="bob@corp.example",
                                     properties={"request": "delete_all_data"}))
        blob = json.dumps(r, default=lambda o: o.__dict__)
        self.assertNotIn("bob@corp", blob)
        self.assertIn("b***@corp.example", blob)

    def test_unparseable_lines_are_masked_too(self):
        r = analyze_bytes(b'{"event_id":"e9","properties":{"first_name":"Maria","email":"m.g@x.com"}\n'
                          b'{"event_id":"e8","x":"bob@corp.example\xff"}\n')
        blob = json.dumps(r, default=lambda o: o.__dict__)
        for leak in ("Maria", "m.g@x.com", "bob@corp"):
            self.assertNotIn(leak, blob)
        self.assertEqual(by_class(r), {"MALFORMED_JSON": {"e9"}, "INVALID_ENCODING": {None}})

    def test_pii_outside_properties_is_flagged(self):
        r = self.run_lines(self.base(traits={"email": "bob@corp.example"}))
        self.assertEqual(by_class(r), {"PII_IN_PAYLOAD": {"e1"}})
        self.assertIn("traits.email", r.findings[0].detail)

    # Identifiers
    def test_unhashable_ids_do_not_crash(self):
        found = self.found(self.base(tenant_id={"x": 1}),
                           self.base(event_id="e2", anonymous_id=["a"]))
        self.assertEqual(found, {"MISSING_TENANT": {"e1"}, "BAD_IDENTIFIER": {"e2"}})

    def test_integer_ids_still_stitch(self):
        r = self.run_lines(self.base(type="identify", user_id=12345))
        self.assertEqual(by_class(r), {})
        self.assertEqual(r.identity_graph, {"t-1/12345": ["a1"]})

    # Bots
    def test_bot_referrer_matches_whole_host_labels(self):
        for ref in ("https://robotics.com/blog", "https://www.abbott.com", "https://scanmarket.io"):
            self.assertEqual(self.found(self.base(properties={"path": "/", "referrer": ref})), {}, ref)
        found = self.found(self.base(properties={"path": "/", "referrer": "https://crawler.example.net/x"}))
        self.assertEqual(found, {"BOT_SUSPECTED": {"e1"}})

    def test_events_without_anonymous_id_are_not_one_bot(self):
        views = [self.base(event_id=f"s{i}", anonymous_id=None, user_id=f"u{i}",
                           received_at=f"2026-06-15T14:00:00.{i}00Z") for i in range(4)]
        self.assertEqual(self.found(*views), {})

    def test_three_fast_views_is_not_a_bot(self):
        self.assertEqual(self.found(*[self.base(event_id=f"e{i}", ts=f"2026-06-15T14:00:00.{i}00Z",
                                                received_at=f"2026-06-15T14:00:00.{i}50Z")
                                      for i in range(3)]), {})

    def test_bot_burst_uses_server_time_and_tolerates_missing_ts(self):
        views = [self.base(event_id=f"b{i}", ts=None, received_at=f"2026-06-15T14:00:00.{i}00Z")
                 for i in range(4)]
        self.assertEqual(self.found(*views), {"BOT_SUSPECTED": {"b0", "b1", "b2", "b3"}})

    # Deletion scope
    def test_privacy_request_without_user_scopes_to_that_anon_only(self):
        r = self.run_lines(
            self.base(event_id="p", type="privacy_request", anonymous_id="a9", properties={}),
            self.base(event_id="mine", anonymous_id="a9"),
            self.base(event_id="other", anonymous_id="a2"),
        )
        scope = next(f for f in r.findings if f.anomaly == "DELETION_SCOPE")
        self.assertIn("mine", scope.detail)
        self.assertNotIn("other", scope.detail)

    def test_deletion_never_reaches_another_users_events_on_a_shared_device(self):
        r = self.run_lines(
            self.base(event_id="u1-id", type="identify", user_id="u1"),
            self.base(event_id="u2-id", type="identify", user_id="u2"),
            self.base(event_id="anon-view", ts="2026-06-15T14:01:00.000Z",
                      received_at="2026-06-15T14:01:00.200Z"),
            self.base(event_id="p", type="privacy_request", user_id="u1", properties={}),
        )
        scope = next(f for f in r.findings if f.anomaly == "DELETION_SCOPE").detail
        touched, _, held = scope.partition("held for a person")
        self.assertIn("u1-id", touched)
        self.assertNotIn("u2-id", scope)
        self.assertIn("anon-view", held)
        self.assertIn("a1 also used by u2", scope)

    def test_privacy_request_with_no_subject_deletes_nothing(self):
        r = self.run_lines(self.base(event_id="p", type="privacy_request", anonymous_id=None,
                                     properties={}), self.base(event_id="x"))
        scope = next(f for f in r.findings if f.anomaly == "DELETION_SCOPE")
        self.assertIn("cannot scope", scope.detail)


class Cli(unittest.TestCase):
    def run_cli(self, *argv):
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = main(list(argv))
        return code, buf.getvalue()

    def test_fail_on_sets_exit_code(self):
        self.assertEqual(self.run_cli(str(FIXTURE))[0], 0)
        self.assertEqual(self.run_cli(str(FIXTURE), "--fail-on", "compliance")[0], 1)
        self.assertEqual(self.run_cli(str(FIXTURE), "--fail-on", "alert")[0], 0)

    def test_missing_file_is_a_usage_error_not_a_traceback(self):
        with self.assertRaises(SystemExit) as ctx, redirect_stdout(io.StringIO()):
            import contextlib
            with contextlib.redirect_stderr(io.StringIO()):
                main(["does-not-exist.jsonl"])
        self.assertEqual(ctx.exception.code, 2)

    def test_markdown_table_escapes_pipes(self):
        rec = {"event_id": "a|b", "tenant_id": "t", "anonymous_id": "x", "type": "nope",
               "ts": "2026-06-15T14:00:00Z", "received_at": "2026-06-15T14:00:00Z", "properties": {}}
        from detect import render_markdown
        md = render_markdown(analyze_bytes(json.dumps(rec).encode()))
        self.assertIn("a\\|b", md)


if __name__ == "__main__":
    unittest.main()
