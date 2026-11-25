"""Профиль первой фазы для чтения Директа и Метрики через MCP."""

import asyncio
import json
import logging
import os
import re
import ssl
import sys
from dataclasses import dataclass, field

import certifi
import httpx
from jsonschema import Draft202012Validator
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import CallToolResult, TextContent, Tool, ToolAnnotations

from server import (
    TOOLS,
    _handle_adgroups_get,
    _handle_ads_get,
    _handle_campaigns_get,
    _handle_keywords_get,
    _handle_report,
)
from tools_direct_extra import request_with_retry
from tools_metrika import METRIKA_TOOLS, register_metrika_handlers

_DIRECT = "https://api.direct.yandex.com/json"
_METRIKA = "https://api-metrika.yandex.net"
_DIRECT_GET = {
    "yd_campaigns_get": ("v5/campaigns", _handle_campaigns_get),
    "yd_adgroups_get": ("v5/adgroups", _handle_adgroups_get),
    "yd_ads_get": ("v501/ads", _handle_ads_get),
    "yd_keywords_get": ("v5/keywords", _handle_keywords_get),
}
_METRIKA_GET = {
    "yd_metrika_counter_get": "/management/v1/counter/{counter_id}",
    "yd_metrika_goals_get": "/management/v1/counter/{counter_id}/goals",
    "yd_metrika_report": "/stat/v1/data",
}
_ALLOWED_TOOLS = {*_DIRECT_GET, "yd_report", *_METRIKA_GET}
_METRIKA_REPORT_PARAMS = {"id", "metrics", "dimensions", "date1", "date2", "filters", "sort", "limit"}
_REPORT_PARAMS = {
    "SelectionCriteria", "FieldNames", "ReportName", "ReportType",
    "DateRangeType", "Format", "IncludeVAT", "IncludeDiscount",
}
_METHOD_OVERRIDE_HEADERS = {"x-http-method", "x-http-method-override", "x-method-override"}


class InvalidConfiguration(ValueError):
    pass


class RequestDenied(ValueError):
    pass


class UpstreamError(Exception):
    pass


@dataclass(frozen=True)
class Settings:
    direct_token: str = field(repr=False)
    metrika_token: str = field(repr=False)
    client_id: int
    counter_id: int
    login: str = field(default="", repr=False)

    @classmethod
    def from_env(cls, env):
        ids = ("YD_PHASE1_DIRECT_CLIENT_ID", "YD_PHASE1_METRIKA_COUNTER_ID")
        tokens = ("YD_OAUTH_TOKEN", "YD_METRIKA_TOKEN")
        for name in ids:
            if not re.fullmatch(r"[1-9][0-9]{0,18}", env.get(name, "")):
                raise InvalidConfiguration(f"Укажите положительный ID в {name}.")
        for name in tokens:
            if not re.fullmatch(r"[!-~]+", env.get(name, "")):
                raise InvalidConfiguration(f"Укажите значение {name} без пробелов.")
        if env[tokens[0]] == env[tokens[1]]:
            raise InvalidConfiguration("Для Директа и Метрики нужны отдельные учётные данные.")
        login = env.get("YD_LOGIN", "")
        if login and not re.fullmatch(r"[A-Za-z0-9._@-]+", login):
            raise InvalidConfiguration("Укажите корректный логин в YD_LOGIN.")
        return cls(env[tokens[0]], env[tokens[1]], int(env[ids[0]]), int(env[ids[1]]), login)

    def direct_headers(self):
        headers = {"Authorization": f"Bearer {self.direct_token}", "Accept-Language": "ru"}
        if self.login:
            headers["Client-Login"] = self.login
        return headers


def _unique_keys(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise RequestDenied
    return result


def _is_report_request(params):
    criteria = params.get("SelectionCriteria")
    if (
        not params.keys() <= _REPORT_PARAMS
        or params.get("Format") != "TSV"
        or params.get("DateRangeType") != "CUSTOM_DATE"
        or not isinstance(criteria, dict)
        or not criteria.keys() <= {"DateFrom", "DateTo", "Filter"}
    ):
        return False
    filters = criteria.get("Filter", [])
    return isinstance(filters, list) and all(
        isinstance(item, dict)
        and item.keys() == {"Field", "Operator", "Values"}
        and item["Field"] == "CampaignId"
        and item["Operator"] == "IN"
        for item in filters
    )


def _guard_http(url, headers, *, operation, counter_id=None):
    """Check the actual request immediately before HTTPX sends it."""
    expected_url = httpx.URL(url)
    report_query = counter_id is not None

    async def check(request):
        target = request.url.copy_with(query=None) if report_query else request.url
        method = "GET" if operation == "metrika" else "POST"
        if target != expected_url or request.method != method:
            raise RequestDenied
        if (
            request.headers.get_list("host") != [expected_url.host]
            or _METHOD_OVERRIDE_HEADERS.intersection(request.headers)
        ):
            raise RequestDenied
        for name in ("Authorization", "Client-Login"):
            expected = [headers[name]] if name in headers else []
            if request.headers.get_list(name) != expected:
                raise RequestDenied
        if operation == "metrika":
            if request.content:
                raise RequestDenied
            if report_query and (
                request.url.params.get_list("id") != [str(counter_id)]
                or not request.url.params.keys() <= _METRIKA_REPORT_PARAMS
            ):
                raise RequestDenied
            return

        if request.headers.get_list("content-type") != ["application/json"]:
            raise RequestDenied
        body = json.loads(request.content, object_pairs_hook=_unique_keys)
        if not isinstance(body, dict) or not isinstance(body.get("params"), dict):
            raise RequestDenied
        if operation == "report":
            if body.keys() != {"params"} or not _is_report_request(body["params"]):
                raise RequestDenied
        elif body.keys() != {"method", "params"} or body["method"] != "get":
            raise RequestDenied
        elif expected_url.path.endswith("/clients") and body["params"] != {"FieldNames": ["ClientId"]}:
            raise RequestDenied

    return check


def _tls_context():
    # A manually created context does not honor SSLKEYLOGFILE.
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(cafile=certifi.where())
    return context


def _http_client(url, headers, *, operation, transport, counter_id=None):
    guard = _guard_http(url, headers, operation=operation, counter_id=counter_id)
    async def check_response(response):
        # Stop the shared report handler before it can return an upstream error body.
        allowed = (200, 201, 202) if operation == "report" else (200, 204)
        if response.status_code not in allowed:
            raise UpstreamError

    response_hooks = [] if operation == "get" else [check_response]
    return httpx.AsyncClient(
        transport=transport,
        verify=_tls_context() if transport is None else True,
        trust_env=False,
        follow_redirects=False,
        event_hooks={"request": [guard], "response": response_hooks},
    )


async def _direct_json(url, headers, params, client):
    response = await request_with_retry(
        client, url, headers=headers, json_body={"method": "get", "params": params}, timeout=120,
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or "error" in data or not isinstance(data.get("result"), dict):
        raise UpstreamError
    return data


async def _require_direct_cabinet(settings, headers, transport):
    url = f"{_DIRECT}/v5/clients"
    async with _http_client(url, headers, operation="get", transport=transport) as client:
        data = await _direct_json(url, headers, {"FieldNames": ["ClientId"]}, client)
    clients = data["result"].get("Clients")
    if not isinstance(clients, list) or len(clients) != 1 or not isinstance(clients[0], dict):
        raise RequestDenied
    client_id = clients[0].get("ClientId")
    if type(client_id) is not int or client_id != settings.client_id:
        raise RequestDenied


def _error(message):
    return CallToolResult(isError=True, content=[TextContent(type="text", text=message)])


def create_server(settings, *, transport=None):
    mcp = Server("yandex-ads-phase1")
    tools = {}
    for tool in (*TOOLS, *METRIKA_TOOLS):
        if tool.name in _ALLOWED_TOOLS:
            tools[tool.name] = Tool(
                name=tool.name,
                description=tool.description,
                inputSchema={**tool.inputSchema, "additionalProperties": False},
                annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
            )
    validators = {name: Draft202012Validator(tool.inputSchema) for name, tool in tools.items()}
    metrika_handlers = {}
    register_metrika_handlers(metrika_handlers, settings.metrika_token)
    direct_headers = settings.direct_headers()
    metrika_headers = {"Authorization": f"OAuth {settings.metrika_token}"}

    @mcp.list_tools()
    async def list_tools():
        return list(tools.values())

    @mcp.call_tool(validate_input=False)
    async def call_tool(name, arguments):
        # SDK validation echoes supplied values. Return fixed errors from this profile instead.
        validator = validators.get(name)
        if validator is None:
            return _error("Инструмент недоступен в режиме только чтения.")
        if not validator.is_valid(arguments):
            return _error("Недопустимые параметры инструмента.")

        try:
            if name in _METRIKA_GET:
                if type(arguments["counter_id"]) is not int or arguments["counter_id"] != settings.counter_id:
                    raise RequestDenied
                url = _METRIKA + _METRIKA_GET[name].format(counter_id=settings.counter_id)
                async with _http_client(
                    url, metrika_headers, operation="metrika", transport=transport,
                    counter_id=settings.counter_id if name == "yd_metrika_report" else None,
                ) as client:
                    return await metrika_handlers[name](client, arguments)

            await _require_direct_cabinet(settings, direct_headers, transport)
            if name == "yd_report":
                base_url = f"{_DIRECT}/v5"
                async with _http_client(
                    f"{base_url}/reports", direct_headers, operation="report", transport=transport,
                ) as client:
                    return await _handle_report(
                        client, arguments, base_url=base_url, headers=direct_headers, raise_on_timeout=True,
                    )

            path, handler = _DIRECT_GET[name]
            url = f"{_DIRECT}/{path}"

            async def direct_get(client, service, method, params):
                if service != path.split("/")[-1] or method != "get":
                    raise RequestDenied
                return await _direct_json(url, direct_headers, params, client)

            async with _http_client(url, direct_headers, operation="get", transport=transport) as client:
                return await handler(client, arguments, api=direct_get)
        except RequestDenied:
            return _error("Запрос выходит за пределы разрешённого доступа.")
        except Exception:
            return _error("Не удалось получить данные из API Яндекса.")

    return mcp


async def run(settings, *, transport=None):
    mcp = create_server(settings, transport=transport)
    async with stdio_server() as (read_stream, write_stream):
        await mcp.run(read_stream, write_stream, mcp.create_initialization_options())


def main_sync():
    # The stdio SDK can log tool names or invalid request payloads. This process logs no payloads.
    logging.disable(logging.CRITICAL)
    try:
        settings = Settings.from_env(os.environ)
        asyncio.run(run(settings))
    except InvalidConfiguration as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from None
    except Exception:
        print("Не удалось запустить MCP-профиль первой фазы.", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main_sync()
