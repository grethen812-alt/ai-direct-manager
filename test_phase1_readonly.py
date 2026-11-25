"""Проверки профиля первой фазы. HTTP всегда подменяется синтетическими ответами."""

from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

import phase1_readonly as profile


ENV = {
    "YD_OAUTH_TOKEN": "fixture-direct-only",
    "YD_METRIKA_TOKEN": "fixture-metrika-only",
    "YD_PHASE1_DIRECT_CLIENT_ID": "101",
    "YD_PHASE1_METRIKA_COUNTER_ID": "202",
    "YD_LOGIN": "fixture-advertiser",
}
SETTINGS = profile.Settings.from_env(ENV)
REPORT = {
    "date_from": "2026-11-01",
    "date_to": "2026-11-02",
    "report_type": "CAMPAIGN_PERFORMANCE_REPORT",
    "field_names": ["CampaignId", "Clicks", "Cost"],
}
METRIKA_REPORT = {"counter_id": 202, "metrics": ["ym:s:visits"]}
TOOLS = {
    "yd_campaigns_get",
    "yd_adgroups_get",
    "yd_ads_get",
    "yd_keywords_get",
    "yd_report",
    "yd_metrika_counter_get",
    "yd_metrika_goals_get",
    "yd_metrika_report",
}
PROBE = "/json/v5/clients"
FIXTURE_SECRET = "fixture-private-error-body"


class FixtureAPI(httpx.MockTransport):
    def __init__(self, record_path):
        super().__init__(self.respond)
        self.record_path = Path(record_path)
        self.report_pending = False

    def respond(self, request):
        path = request.url.path
        body = json.loads(request.content) if request.content else None
        with self.record_path.open("a", encoding="utf-8") as record:
            record.write(path + "\n")
        if path.startswith("/json/"):
            assert request.headers["authorization"] == "Bearer " + ENV["YD_OAUTH_TOKEN"]
            assert request.headers["client-login"] == ENV["YD_LOGIN"]
        else:
            assert request.headers["authorization"] == "OAuth " + ENV["YD_METRIKA_TOKEN"]
            assert "client-login" not in request.headers
        if path == PROBE:
            assert body == {"method": "get", "params": {"FieldNames": ["ClientId"]}}
            client_id = int(os.environ.get("FIXTURE_DIRECT_ID", "101"))
            return httpx.Response(200, json={"result": {"Clients": [{"ClientId": client_id}]}})
        if path.endswith("/reports"):
            assert body["params"]["Format"] == "TSV"
            if body["params"]["FieldNames"] == ["FixtureHttpError"]:
                return httpx.Response(400, text=FIXTURE_SECRET)
            if body["params"]["FieldNames"] == ["FixtureRedirect"]:
                return httpx.Response(302, headers={"location": "https://example.test/"}, text=FIXTURE_SECRET)
            if not self.report_pending:
                self.report_pending = True
                return httpx.Response(201, headers={"retryIn": "0"})
            return httpx.Response(200, text="CampaignId\tClicks\tCost\n1\t12\t240.00\n")
        for service, collection in [
            ("campaigns", "Campaigns"), ("adgroups", "AdGroups"), ("ads", "Ads"), ("keywords", "Keywords"),
        ]:
            if path.endswith("/" + service):
                assert body["method"] == "get"
                if body["params"]["SelectionCriteria"].get("Ids") == [999]:
                    return httpx.Response(200, json={"error": {"error_detail": FIXTURE_SECRET}})
                return httpx.Response(200, json={"result": {collection: [{"Id": 1}]}})
        if path == "/management/v1/counter/202":
            return httpx.Response(200, json={"counter": {"id": 202, "name": "Синтетический счётчик"}})
        if path == "/management/v1/counter/202/goals":
            return httpx.Response(200, json={"goals": [{"id": 303, "name": "Синтетическая цель"}]})
        if path == "/stat/v1/data":
            assert request.url.params.get_list("id") == ["202"]
            if request.url.params["metrics"] == "fixture:error":
                return httpx.Response(403, text=FIXTURE_SECRET)
            return httpx.Response(200, json={"data": [{"metrics": [9]}], "sampled": False})
        raise AssertionError("Unexpected outbound path")


def run_fixture_server():
    run = profile.run
    transport = FixtureAPI(os.environ["FIXTURE_RECORD"])

    async def run_offline(settings):
        await run(settings, transport=transport)

    with patch.object(profile, "run", run_offline):
        profile.main_sync()


class ProfileTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.record = Path(directory.name) / "requests.txt"
        self.log_file = Path(directory.name) / "legacy.log"

    def requests(self):
        if not self.record.exists():
            return []
        return self.record.read_text().splitlines()

    @asynccontextmanager
    async def connect(self, **extra_env):
        env = {
            **ENV,
            "FIXTURE_RECORD": str(self.record),
            "YD_READONLY": "false",
            "YD_CONFIRM": "true",
            "YD_LOG_LEVEL": "DEBUG",
            "YD_LOG_BODIES": "true",
            "YD_LOG_FILE": str(self.log_file),
            "YD_API_URL": "https://wrong-host.example.test",
            "HTTPS_PROXY": "http://127.0.0.1:1",
            "YD_DIRECT_TOKENS": "foreign:fixture-unused-token",
            **extra_env,
        }
        params = StdioServerParameters(
            command=sys.executable, args=[str(Path(__file__).resolve()), "--fixture-server"], env=env,
        )
        with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as self.errlog:
            async with stdio_client(params, errlog=self.errlog) as streams, ClientSession(*streams) as session:
                init = await session.initialize()
                self.assertEqual(init.serverInfo.name, "yandex-ads-phase1")
                yield session

    async def test_all_eight_reads_over_stdio(self):
        async with self.connect() as session:
            listed = (await session.list_tools()).tools
            self.assertEqual({tool.name for tool in listed}, TOOLS)
            for tool in listed:
                self.assertTrue(tool.annotations.readOnlyHint)
                self.assertFalse(tool.inputSchema["additionalProperties"])
                self.assertNotIn("client_login", tool.inputSchema["properties"])

            for name, args, path, expected in [
                ("yd_campaigns_get", {}, "/json/v5/campaigns", "Campaigns"),
                ("yd_adgroups_get", {"campaign_ids": [1]}, "/json/v5/adgroups", "AdGroups"),
                ("yd_ads_get", {"ad_ids": ["9007199254740993"]}, "/json/v501/ads", "Ads"),
                ("yd_keywords_get", {"campaign_ids": [1]}, "/json/v5/keywords", "Keywords"),
                ("yd_report", REPORT, "/json/v5/reports", "1\t12\t240.00"),
                ("yd_metrika_counter_get", {"counter_id": 202}, "/management/v1/counter/202", "Синтетический счётчик"),
                ("yd_metrika_goals_get", {"counter_id": 202}, "/management/v1/counter/202/goals", "Синтетическая цель"),
                ("yd_metrika_report", METRIKA_REPORT, "/stat/v1/data", '"sampled": false'),
            ]:
                with self.subTest(tool=name):
                    result = await session.call_tool(name, args)
                    self.assertFalse(result.isError, result.content)
                    self.assertIn(expected, result.content[0].text)
                    self.assertEqual(self.requests()[-1], path)
            self.assertEqual(self.requests().count(PROBE), 5)
            self.assertEqual(self.requests().count("/json/v5/reports"), 2)
            self.assertFalse(self.log_file.exists())

    async def test_rejects_writes_unknown_tools_and_scope_overrides_before_http(self):
        async with self.connect() as session:
            attempts = [
                ("yd_campaigns_update", {"campaigns": [{"id": 1}], "confirm": True}),
                ("yd_campaigns_action", {"action": "suspend", "confirm": True}),
                ("yd_metrika_goal_delete", {"counter_id": 202, "goal_id": 303}),
                ("yd_videos_upload", {"file_path": str(self.record)}),
                ("yd_audience_segment_upload", {"file_path": str(self.record)}),
                ("yd_direct_accounts_get", {}),
                (FIXTURE_SECRET, {}),
                ("yd_campaigns_get", {"client_login": "foreign"}),
                ("yd_campaigns_get", {"client_login": ENV["YD_LOGIN"]}),
                ("yd_campaigns_get", {"method": "update", "confirm": True}),
                ("yd_campaigns_get", {"file_path": str(self.record)}),
                ("yd_campaigns_get", {"ids": [FIXTURE_SECRET]}),
                ("yd_metrika_counter_get", {"counter_id": 404}),
                ("yd_metrika_goals_get", {"counter_id": "202/../404"}),
                ("yd_metrika_report", {**METRIKA_REPORT, "counter_id": 404}),
                ("yd_metrika_report", {**METRIKA_REPORT, "counter_id": 202.0}),
                ("yd_metrika_report", {**METRIKA_REPORT, "id": 404}),
            ]
            for name, args in attempts:
                with self.subTest(tool=name):
                    result = await session.call_tool(name, args)
                    self.assertTrue(result.isError)
                    self.assertNotIn(FIXTURE_SECRET, result.content[0].text)
            self.assertEqual(self.requests(), [])
            self.errlog.seek(0)
            self.assertEqual(self.errlog.read(), "")
            self.assertFalse(self.log_file.exists())

    async def test_wrong_direct_credential_stops_after_identity_probe(self):
        async with self.connect(FIXTURE_DIRECT_ID="404") as session:
            result = await session.call_tool("yd_report", REPORT)
            self.assertTrue(result.isError)
            self.assertEqual(self.requests(), [PROBE])
            self.assertNotIn("404", result.content[0].text)

    async def test_upstream_errors_and_redirects_are_redacted(self):
        async with self.connect() as session:
            for name, args in [
                ("yd_campaigns_get", {"ids": [999]}),
                ("yd_report", {**REPORT, "field_names": ["FixtureHttpError"]}),
                ("yd_report", {**REPORT, "field_names": ["FixtureRedirect"]}),
                ("yd_metrika_report", {**METRIKA_REPORT, "metrics": ["fixture:error"]}),
            ]:
                result = await session.call_tool(name, args)
                self.assertTrue(result.isError)
                self.assertEqual(result.content[0].text, "Не удалось получить данные из API Яндекса.")
            self.assertEqual(self.requests(), [PROBE, "/json/v5/campaigns", PROBE, "/json/v5/reports",
                                              PROBE, "/json/v5/reports", "/stat/v1/data"])
            self.errlog.seek(0)
            self.assertEqual(self.errlog.read(), "")

    async def test_http_gate_blocks_requests_even_without_mcp_validation(self):
        def reject_network(request):
            self.fail("Unsafe request reached the transport")

        direct = "https://api.direct.yandex.com/json/v5/campaigns"
        headers = SETTINGS.direct_headers()
        mock = httpx.MockTransport(reject_network)
        async with profile._http_client(direct, headers, operation="get", transport=mock) as client:
            body = {"method": "get", "params": {}}
            for method, url, sent_headers, payload in [
                ("PUT", direct, headers, body),
                ("POST", direct, headers, {**body, "method": "update"}),
                ("POST", direct, headers, [{**body, "method": "get"}, {**body, "method": "update"}]),
                ("POST", direct, {**headers, "Client-Login": "foreign"}, body),
                ("POST", direct, {**headers, "Authorization": "Bearer other"}, body),
                ("POST", direct, {**headers, "Host": "other.example.test"}, body),
                ("POST", direct, {**headers, "X-HTTP-Method-Override": "DELETE"}, body),
                ("POST", direct.replace("https:", "http:"), headers, body),
                ("POST", direct + "?client_login=foreign", headers, body),
                ("POST", direct.replace("api.direct.yandex.com", "example.test"), headers, body),
                ("POST", direct.replace("campaigns", "clients"), headers, body),
            ]:
                with self.subTest(method=method, url=url), self.assertRaises(profile.RequestDenied):
                    await client.request(method, url, headers=sent_headers, json=payload)
            with self.assertRaises(profile.RequestDenied):
                await client.post(
                    direct,
                    headers={**headers, "Content-Type": "application/json"},
                    content=b'{"method":"update","method":"get","params":{}}',
                )

        report = "https://api.direct.yandex.com/json/v5/reports"
        async with profile._http_client(report, headers, operation="report", transport=mock) as client:
            with self.assertRaises(profile.RequestDenied):
                await client.post(report, headers=headers, json={"method": "add", "params": {"Format": "TSV"}})
            with self.assertRaises(profile.RequestDenied):
                await client.post(report, headers=headers, json={"params": {
                    "Format": "TSV", "DateRangeType": "CUSTOM_DATE",
                    "SelectionCriteria": {"Filter": [
                        {"Field": "ClientLogin", "Operator": "IN", "Values": ["foreign"]},
                    ]},
                }})

        stats = "https://api-metrika.yandex.net/stat/v1/data"
        headers = {"Authorization": "OAuth " + ENV["YD_METRIKA_TOKEN"]}
        async with profile._http_client(stats, headers, operation="metrika", transport=mock, counter_id=202) as client:
            for params in [{"id": 404}, {"id": [202, 404]}, {"id": 202, "other_id": 404}]:
                with self.subTest(params=params), self.assertRaises(profile.RequestDenied):
                    await client.get(stats, headers=headers, params=params)
            with self.assertRaises(profile.RequestDenied):
                await client.delete(stats, headers=headers, params={"id": 202})


class StartupTest(unittest.TestCase):
    def test_incomplete_invalid_or_shared_credentials_are_rejected(self):
        for name in ("YD_OAUTH_TOKEN", "YD_METRIKA_TOKEN", "YD_PHASE1_DIRECT_CLIENT_ID", "YD_PHASE1_METRIKA_COUNTER_ID"):
            with self.subTest(missing=name), self.assertRaises(profile.InvalidConfiguration):
                profile.Settings.from_env({**ENV, name: ""})
        for changed in [
            {"YD_PHASE1_METRIKA_COUNTER_ID": "202/404"},
            {"YD_LOGIN": "fixture\ninvalid"},
            {"YD_METRIKA_TOKEN": ENV["YD_OAUTH_TOKEN"]},
        ]:
            with self.subTest(changed=changed), self.assertRaises(profile.InvalidConfiguration):
                profile.Settings.from_env({**ENV, **changed})
        token_bound = profile.Settings.from_env({**ENV, "YD_LOGIN": ""})
        self.assertNotIn("Client-Login", token_bound.direct_headers())

    def test_tls_context_ignores_key_logging_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            keylog = Path(directory) / "keys.log"
            with patch.dict(os.environ, {"SSLKEYLOGFILE": str(keylog)}):
                context = profile._tls_context()
            self.assertIsNone(context.keylog_filename)
            self.assertFalse(keylog.exists())

    def test_command_fails_closed_with_sanitized_stderr(self):
        env = {key: value for key, value in os.environ.items() if not key.startswith("YD_")}
        env["YD_PHASE1_DIRECT_CLIENT_ID"] = FIXTURE_SECRET
        process = subprocess.run(
            [sys.executable, "-m", "phase1_readonly"], env=env,
            capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(process.returncode, 1)
        self.assertEqual(process.stdout, "")
        self.assertIn("YD_PHASE1_DIRECT_CLIENT_ID", process.stderr)
        self.assertNotIn(FIXTURE_SECRET, process.stderr)
        self.assertNotIn("Traceback", process.stderr)


if __name__ == "__main__":
    if sys.argv[1:] == ["--fixture-server"]:
        run_fixture_server()
    else:
        unittest.main()
