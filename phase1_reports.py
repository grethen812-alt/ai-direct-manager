"""Сроки, объём и проверка ответов аналитики первой фазы."""

import asyncio
import csv
import io
import json
import re
from datetime import date, datetime, timedelta, timezone

from server import _report_body
from tools_metrika import _report_params

DEADLINE_SECONDS = 90
MAX_RETRY_SECONDS = 5
MAX_DAYS = 31
MAX_ROWS = 1_000
MAX_BYTES = 128 * 1_024
DIRECT_PAGE = {"Limit": MAX_ROWS + 1}


class ReportError(ValueError):
    """Fixed, user-facing error without upstream data."""


_FORMAT_ERROR = "API Яндекса вернул отчёт в неожиданном формате."
_SIZE_ERROR = f"Отчёт превышает {MAX_BYTES // 1024} КиБ. Сузьте период или сократите набор полей."


def _parse_date(value):
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ReportError("Укажите даты в формате ГГГГ-ММ-ДД.")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ReportError("Укажите корректные календарные даты.") from None


def report_arguments(args, *, direct):
    if direct:
        start, end = (_parse_date(args[key]) for key in ("date_from", "date_to"))
        fields = args["field_names"]
        if not fields or any(not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", field) for field in fields):
            raise ReportError("Укажите непустой список имён полей Директа.")
    else:
        end = _parse_date(args["date2"]) if "date2" in args else datetime.now(timezone(timedelta(hours=3))).date()
        try:
            start = _parse_date(args["date1"]) if "date1" in args else end - timedelta(days=29)
        except OverflowError:
            raise ReportError("Укажите корректные календарные даты.") from None
        limit = args.get("limit", 100)
        if type(limit) is not int or not 1 <= limit <= MAX_ROWS:
            raise ReportError(f"Лимит строк Метрики должен быть от 1 до {MAX_ROWS}.")
        metrics = args["metrics"]
        dimensions = args.get("dimensions", [])
        if not metrics or any(not field.strip() or "," in field for field in metrics + dimensions):
            raise ReportError("Передайте каждую метрику и группировку отдельным элементом списка.")
        args = {**args, "date1": start.isoformat(), "date2": end.isoformat(), "limit": limit}
    if start > end or (end - start).days >= MAX_DAYS:
        raise ReportError(f"Укажите период до {MAX_DAYS} календарного дня включительно с датой начала не позже даты окончания.")
    return args


def _rows_error(limit):
    return ReportError(f"Отчёт превышает лимит {limit} строк. Сузьте период или добавьте фильтр.")


def _retry_delay(value):
    if not value.isascii() or not value.isdecimal():
        return MAX_RETRY_SECONDS
    # Compare as text after stripping zeros so an arbitrarily long value cannot overflow int().
    seconds = value.lstrip("0")
    if not seconds:
        return 1
    return min(MAX_RETRY_SECONDS, int(seconds)) if len(seconds) == 1 else MAX_RETRY_SECONDS


async def _body(response, media_types):
    if response.headers.get("content-encoding", "identity").lower() != "identity":
        raise ReportError(_FORMAT_ERROR)
    if response.headers.get("content-type", "").split(";", 1)[0].strip().lower() not in media_types:
        raise ReportError(_FORMAT_ERROR)
    data = bytearray()
    async for chunk in response.aiter_bytes():
        if len(data) + len(chunk) > MAX_BYTES:
            raise ReportError(_SIZE_ERROR)
        data.extend(chunk)
    try:
        return data.decode("utf-8-sig")
    except UnicodeError:
        raise ReportError(_FORMAT_ERROR) from None


def _tsv(text, fields):
    try:
        rows = csv.reader(io.StringIO(text, newline=""), delimiter="\t", strict=True)
        if next(rows, None) != fields:
            raise ReportError(_FORMAT_ERROR)
        for number, row in enumerate(rows, 1):
            if number > MAX_ROWS:
                raise _rows_error(MAX_ROWS)
            if len(row) != len(fields):
                raise ReportError(_FORMAT_ERROR)
    except csv.Error:
        raise ReportError(_FORMAT_ERROR) from None
    return text


def _unique_json_keys(pairs):
    obj = dict(pairs)
    if len(obj) != len(pairs):
        raise ReportError(_FORMAT_ERROR)
    return obj


def _invalid_json_constant(_):
    raise ReportError(_FORMAT_ERROR)


def _metrika_json(text, args):
    try:
        data = json.loads(text, object_pairs_hook=_unique_json_keys, parse_constant=_invalid_json_constant)
        if not isinstance(data, dict) or not isinstance(data.get("data"), list):
            raise ReportError(_FORMAT_ERROR)
        total = data.get("total_rows")
        rows = data["data"]
        if type(total) is not int or total < 0 or type(data.get("sampled")) is not bool:
            raise ReportError(_FORMAT_ERROR)
        if total > args["limit"] or len(rows) > args["limit"]:
            raise _rows_error(args["limit"])
        rounded = data.get("total_rows_rounded", False)
        if type(rounded) is not bool:
            raise ReportError(_FORMAT_ERROR)
        if rounded or total > len(rows):
            raise ReportError("API Метрики вернул неполный отчёт или округлённое число строк. Сузьте период или добавьте фильтр.")
        if total < len(rows):
            raise ReportError(_FORMAT_ERROR)

        metrics_count = len(args["metrics"])
        dimensions_count = len(args.get("dimensions", []))
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get("metrics"), list):
                raise ReportError(_FORMAT_ERROR)
            values = row["metrics"]
            dimensions = row.get("dimensions", [])
            if (
                len(values) != metrics_count
                or any(type(value) not in (int, float) for value in values)
                or not isinstance(dimensions, list)
                or len(dimensions) != dimensions_count
                or any(not isinstance(dimension, dict) for dimension in dimensions)
            ):
                raise ReportError(_FORMAT_ERROR)
        result = json.dumps(data, ensure_ascii=False, allow_nan=False, separators=(", ", ": "))
        if len(result.encode("utf-8")) > MAX_BYTES:
            raise ReportError(_SIZE_ERROR)
        return result
    except ReportError:
        raise
    except (ValueError, RecursionError):
        raise ReportError(_FORMAT_ERROR) from None


async def read_direct_report(client, url, headers, args):
    body = _report_body(args)
    body["params"]["Page"] = DIRECT_PAGE
    headers = {
        **headers,
        "processingMode": "auto",
        "returnMoneyInMicros": "false",
        "skipReportHeader": "true",
        "skipReportSummary": "true",
        "Accept-Encoding": "identity",
    }
    for attempt in range(30):
        async with client.stream("POST", url, headers=headers, json=body, timeout=30) as response:
            if response.status_code == 200:
                text = await _body(response, {"text/tab-separated-values", "text/plain"})
                return _tsv(text, args["field_names"])
            # The phase-one HTTP client only admits 200, 201 or 202 for this endpoint.
            delay = _retry_delay(response.headers.get("retryIn", "5"))
        if attempt < 29:
            await asyncio.sleep(delay)
    raise ReportError("Отчёт Директа ещё не готов после 30 запросов. Сузьте период или повторите запрос позже.")


async def read_metrika_report(client, url, headers, args):
    params = _report_params({**args, "limit": args["limit"] + 1})
    async with client.stream(
        "GET", url, headers={**headers, "Accept-Encoding": "identity"}, params=params, timeout=30,
    ) as response:
        if response.status_code != 200:
            raise ReportError(_FORMAT_ERROR)
        text = await _body(response, {"application/json"})
    return _metrika_json(text, args)
