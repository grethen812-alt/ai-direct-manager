"""Офлайн-запуск профиля первой фазы для MCP-клиента или частного туннеля."""

import asyncio
import json
import logging
from pathlib import Path

import httpx

from phase1_readonly import Settings, run

DEMO_ENV = {
    "YD_OAUTH_TOKEN": "fixture-direct-only",
    "YD_METRIKA_TOKEN": "fixture-metrika-only",
    "YD_PHASE1_DIRECT_CLIENT_ID": "101",
    "YD_PHASE1_METRIKA_COUNTER_ID": "202",
    "YD_LOGIN": "fixture-advertiser",
}

PROBE = "/json/v5/clients"
FIXTURE_SECRET = "fixture-private-error-body"


class FixtureAPI(httpx.MockTransport):
    def __init__(self, record_path=None, direct_id=101):
        super().__init__(self.respond)
        self.record_path = Path(record_path) if record_path is not None else None
        self.direct_id = direct_id
        self.report_pending = False

    def respond(self, request):
        path = request.url.path
        body = json.loads(request.content) if request.content else None
        if self.record_path is not None:
            with self.record_path.open("a", encoding="utf-8") as record:
                record.write(path + "\n")
        if path.startswith("/json/"):
            assert request.headers["authorization"] == "Bearer " + DEMO_ENV["YD_OAUTH_TOKEN"]
            assert request.headers["client-login"] == DEMO_ENV["YD_LOGIN"]
        else:
            assert request.headers["authorization"] == "OAuth " + DEMO_ENV["YD_METRIKA_TOKEN"]
            assert "client-login" not in request.headers
        if path == PROBE:
            assert body == {"method": "get", "params": {"FieldNames": ["ClientId"]}}
            return httpx.Response(200, json={"result": {"Clients": [{"ClientId": self.direct_id}]}})
        if path.endswith("/reports"):
            assert body["params"]["Format"] == "TSV"
            if body["params"]["FieldNames"] == ["FixtureHttpError"]:
                return httpx.Response(400, text=FIXTURE_SECRET)
            if body["params"]["FieldNames"] == ["FixtureRedirect"]:
                return httpx.Response(302, headers={"location": "https://example.test/"}, text=FIXTURE_SECRET)
            if not self.report_pending:
                self.report_pending = True
                return httpx.Response(201, headers={"retryIn": "0"})
            fields = body["params"]["FieldNames"]
            values = {"CampaignId": "1", "Clicks": "12", "Cost": "240.00"}
            tsv = "\t".join(fields) + "\n" + "\t".join(values.get(field, "0") for field in fields) + "\n"
            return httpx.Response(200, text=tsv, headers={"Content-Type": "text/tab-separated-values"})
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
            metrics = request.url.params["metrics"].split(",")
            dimensions = request.url.params.get("dimensions", "")
            row = {"metrics": [9 if metric == "ym:s:visits" else 0 for metric in metrics]}
            if dimensions:
                row["dimensions"] = [{"name": "Синтетическая группа"} for _ in dimensions.split(",")]
            return httpx.Response(200, json={"data": [row], "sampled": False, "total_rows": 1})
        raise AssertionError("Unexpected outbound path")


def main_sync(*, record_path=None, direct_id=101):
    logging.disable(logging.CRITICAL)
    settings = Settings.from_env(DEMO_ENV)
    asyncio.run(run(settings, transport=FixtureAPI(record_path, direct_id)))


if __name__ == "__main__":
    main_sync()
