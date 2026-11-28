"""Тесты лимитов и форматов отчётов через MCP с подставным HTTP."""

import asyncio
import json
import unittest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import httpx
from mcp.shared.memory import create_connected_server_and_client_session

import phase1_readonly as profile
import phase1_reports as reports
from test_phase1_readonly import METRIKA_REPORT, PROBE, REPORT, SETTINGS


def direct_tsv(data):
    return httpx.Response(200, headers={"Content-Type": "text/tab-separated-values"}, content=data)


def metrika_data(count=1, **meta):
    return {
        "data": [{"metrics": [9]}] * count,
        "total_rows": count,
        "sampled": False,
        **meta,
    }


class Chunks(httpx.AsyncByteStream):
    def __init__(self, *chunks):
        self.chunks = chunks
        self.read_count = 0
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            self.read_count += 1
            yield chunk

    async def aclose(self):
        self.closed = True


class ReportTest(unittest.IsolatedAsyncioTestCase):
    @asynccontextmanager
    async def connect(self, respond):
        self.requests = []

        async def dispatch(request):
            self.requests.append(request)
            if request.url.path == PROBE:
                assert json.loads(request.content)["params"] == {"FieldNames": ["ClientId"]}
                return httpx.Response(200, json={"result": {"Clients": [{"ClientId": 101}]}})
            return await respond(request)

        mcp = profile.create_server(SETTINGS, transport=httpx.MockTransport(dispatch))
        async with create_connected_server_and_client_session(mcp) as session:
            yield session

    async def test_bad_ranges_and_limits_are_rejected_before_http(self):
        async def unexpected(_):
            self.fail("An invalid report reached HTTP")

        async with self.connect(unexpected) as session:
            for name, args in [
                ("yd_report", {**REPORT, "date_from": "2026-02-30"}),
                ("yd_report", {**REPORT, "date_from": "20261101"}),
                ("yd_report", {**REPORT, "date_to": "2026-12-02"}),
                ("yd_report", {**REPORT, "date_to": "2026-10-01"}),
                ("yd_report", {**REPORT, "field_names": []}),
                ("yd_metrika_report", {**METRIKA_REPORT, "date1": "2026-01-01", "date2": "2026-02-01"}),
                ("yd_metrika_report", {**METRIKA_REPORT, "limit": 0}),
                ("yd_metrika_report", {**METRIKA_REPORT, "limit": 1001}),
                ("yd_metrika_report", {**METRIKA_REPORT, "limit": 1.0}),
                ("yd_metrika_report", {**METRIKA_REPORT, "metrics": ["ym:s:visits,ym:s:users"]}),
            ]:
                with self.subTest(tool=name, args=args):
                    self.assertTrue((await session.call_tool(name, args)).isError)
            self.assertEqual(self.requests, [])

    async def test_direct_row_boundary_and_page_request(self):
        row = b"1\t12\t240.00\r\n"
        header = b"CampaignId\tClicks\tCost\r\n"
        for count in (0, reports.MAX_ROWS, reports.MAX_ROWS + 1):
            async def respond(request):
                self.assertEqual(json.loads(request.content)["params"]["Page"], {"Limit": 1001})
                self.assertEqual(request.headers["skipreportheader"], "true")
                self.assertEqual(request.headers["skipreportsummary"], "true")
                self.assertEqual(request.headers["accept-encoding"], "identity")
                return direct_tsv(header + row * count)

            with self.subTest(count=count):
                async with self.connect(respond) as session:
                    result = await session.call_tool("yd_report", {**REPORT, "date_to": "2026-12-01"})
                    if count <= reports.MAX_ROWS:
                        self.assertFalse(result.isError)
                        self.assertEqual(result.content[0].text.encode(), header + row * count)
                    else:
                        self.assertTrue(result.isError)
                        self.assertIn("1000 строк", result.content[0].text)

    async def test_metrika_discloses_incomplete_results(self):
        for payload, expected in [
            (metrika_data(), None),
            (metrika_data(0), None),
            (metrika_data(2), "лимит 1 строк"),
            (metrika_data(total_rows=9), "лимит 1 строк"),
            (metrika_data(0, total_rows=1), "неполный"),
            (metrika_data(total_rows_rounded=True), "округлённое"),
            (metrika_data(total_rows=0), "неожиданном формате"),
        ]:
            async def respond(request):
                self.assertEqual(request.url.params.get_list("limit"), ["2"])
                self.assertEqual(request.url.params["date1"], "2026-11-02")
                self.assertEqual(request.url.params["date2"], "2026-12-01")
                return httpx.Response(200, json=payload)

            with self.subTest(payload=payload):
                async with self.connect(respond) as session:
                    result = await session.call_tool("yd_metrika_report", {
                        **METRIKA_REPORT, "date2": "2026-12-01", "limit": 1,
                    })
                    self.assertEqual(result.isError, expected is not None)
                    if expected:
                        self.assertIn(expected, result.content[0].text)
                        self.assertNotIn('"data"', result.content[0].text)
                    else:
                        self.assertEqual(json.loads(result.content[0].text), payload)

    async def test_malformed_reports_do_not_echo_payload(self):
        for name, args, factory in [
            ("yd_report", REPORT, lambda: httpx.Response(200, json={"error": "hidden"})),
            ("yd_report", REPORT, lambda: direct_tsv(b"CampaignId\tClicks\tCost\n1\t12\n")),
            ("yd_report", REPORT, lambda: direct_tsv(b"CampaignId\tClicks\tCost\n1\t\xff\t0\n")),
            ("yd_report", REPORT, lambda: direct_tsv(b"Clicks\tCost\n12\t240.00\n")),
            ("yd_metrika_report", METRIKA_REPORT, lambda: httpx.Response(200, json={"error": "hidden"})),
            ("yd_metrika_report", METRIKA_REPORT, lambda: httpx.Response(200, json=metrika_data(
                data=[{"metrics": ["hidden"]}],
            ))),
            ("yd_metrika_report", METRIKA_REPORT, lambda: httpx.Response(
                200, headers={"Content-Type": "application/json"}, content=b'{"sampled":false,"data":invalid}',
            )),
            ("yd_metrika_report", METRIKA_REPORT, lambda: httpx.Response(
                200, headers={"Content-Type": "application/json"}, content=b'{"data":[],"sampled":false,"total_rows":0,"total_rows":1}',
            )),
        ]:
            async def respond(_):
                return factory()

            with self.subTest(name=name, factory=factory):
                async with self.connect(respond) as session:
                    result = await session.call_tool(name, args)
                    self.assertTrue(result.isError)
                    self.assertEqual(result.content[0].text, "API Яндекса вернул отчёт в неожиданном формате.")

    async def test_body_limit_stops_stream_and_never_returns_partial_data(self):
        for name, args, media_type in [
            ("yd_report", REPORT, "text/tab-separated-values"),
            ("yd_metrika_report", METRIKA_REPORT, "application/json"),
        ]:
            stream = Chunks(b"x" * reports.MAX_BYTES, b"y", b"z")

            async def respond(_):
                return httpx.Response(200, headers={"Content-Type": media_type}, stream=stream)

            with self.subTest(name=name):
                async with self.connect(respond) as session:
                    result = await session.call_tool(name, args)
                    self.assertTrue(result.isError)
                    self.assertIn("128 КиБ", result.content[0].text)
                    self.assertEqual(stream.read_count, 2)
                    self.assertTrue(stream.closed)

    async def test_exact_byte_limit_succeeds_and_json_expansion_fails(self):
        tsv = b"CampaignId\tClicks\tCost\n"
        body = tsv + b"x" * (reports.MAX_BYTES - len(tsv) - 5) + b"\t0\t0\n"
        self.assertEqual(len(body), reports.MAX_BYTES)

        async def respond(_):
            return direct_tsv(body)

        async with self.connect(respond) as session:
            result = await session.call_tool("yd_report", REPORT)
            self.assertFalse(result.isError)
            self.assertEqual(result.content[0].text.encode(), body)

        payload = metrika_data(extra=[1] * 45_000)
        compact = json.dumps(payload, separators=(",", ":")).encode()
        self.assertLess(len(compact), reports.MAX_BYTES)

        async def compact_response(_):
            return httpx.Response(200, headers={"Content-Type": "application/json"}, content=compact)

        async with self.connect(compact_response) as session:
            result = await session.call_tool("yd_metrika_report", METRIKA_REPORT)
            self.assertTrue(result.isError)
            self.assertIn("128 КиБ", result.content[0].text)

    async def test_report_retries_are_clamped_and_follow_one_deadline(self):
        attempt = 0

        async def respond(_):
            nonlocal attempt
            attempt += 1
            if attempt <= 3:
                hints = ["3600", "0", "invalid"]
                return httpx.Response(202, headers={"retryIn": hints[attempt - 1]})
            return direct_tsv(b"CampaignId\tClicks\tCost\n1\t2\t3\n")

        with patch.object(reports.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            async with self.connect(respond) as session:
                result = await session.call_tool("yd_report", REPORT)
        self.assertFalse(result.isError)
        self.assertEqual([entry.args[0] for entry in sleep.await_args_list], [5, 1, 5])
        self.assertEqual(reports.DEADLINE_SECONDS, 90)

    async def test_deadline_covers_direct_probe_and_pending_report(self):
        for stall_at_probe in (True, False):
            cancelled = asyncio.Event()
            captured = []

            async def respond(request):
                path = request.url.path
                captured.append(path)
                if path == PROBE and not stall_at_probe:
                    return httpx.Response(200, json={"result": {"Clients": [{"ClientId": 101}]}})
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

            with self.subTest(probe=stall_at_probe):
                mcp = profile.create_server(SETTINGS, transport=httpx.MockTransport(respond))
                with patch.object(profile, "DEADLINE_SECONDS", 0.05):
                    async with create_connected_server_and_client_session(mcp) as session:
                        result = await session.call_tool("yd_report", REPORT)
                self.assertTrue(result.isError)
                self.assertIn("не готов за", result.content[0].text)
                self.assertTrue(cancelled.is_set())
                self.assertEqual(captured, [PROBE] if stall_at_probe else [PROBE, "/json/v5/reports"])


if __name__ == "__main__":
    unittest.main()
