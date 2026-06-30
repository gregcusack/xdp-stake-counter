#!/usr/bin/env python3
"""Report how much validator stake is represented by XDP validators."""

from __future__ import annotations

import argparse
import base64
import csv
import html
import io
import json
import os
import re
import ssl
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from fractions import Fraction
from pathlib import Path
from typing import Any


NETWORK_FILES = {
    "testnet": {
        "csv": Path("data/testnet-xdp-data.csv"),
        "stake_json": Path("data/testnet-stake.json"),
        "influx_db": "tds",
        "solana_url": "t",
    },
    "mainnet": {
        "csv": Path("data/mainnet-xdp-data.csv"),
        "stake_json": Path("data/mainnet-stake.json"),
        "influx_db": "mainnet-beta",
        "solana_url": "m",
    },
}
DEFAULT_RETENTION_POLICY = "autogen"
DEFAULT_MEASUREMENT = "retransmit-stage"
DEFAULT_DASHBOARD_MEASUREMENT = "broadcast-transmit-shreds-stats"
DEFAULT_INFLUX_INTERVAL = "1m"
DEFAULT_INFLUX_HOURS = 12
DEFAULT_TOP_STAKED_COUNT = 100
TOP_STAKED_XDP_STATUS_COUNTS = (10, 20, 50, 100)
XDP_SOURCE_RETRANSMIT_STAGE = "retransmit-stage"
XDP_SOURCE_DASHBOARD = "broadcast-transmit-shreds-stats"
VALIDATORS_APP_BASE_URL = "https://www.validators.app"
INFLUX_DURATION_PATTERN = re.compile(r"^[1-9][0-9]*(?:u|us|ms|s|m|h|d|w)$")
VALIDATORS_APP_NAME_LINK_PATTERN = re.compile(
    r'<a\s+class="[^"]*\bcolumn-info-link\b[^"]*"[^>]*'
    r'href="(?:https://www\.validators\.app)?/validators/([^"?]+)\?[^"]*"[^>]*>'
    r"\s*(.*?)\s*(?:<small\b|</a>)",
    re.DOTALL,
)


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        raise FileNotFoundError(f"{env_path} does not exist")

    with env_path.open() as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            if key:
                os.environ[key] = value


def env_value(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def parse_solana_metrics_config() -> dict[str, str]:
    config = env_value("SOLANA_METRICS_CONFIG")
    if not config:
        return {}

    values: dict[str, str] = {}
    for item in config.split(","):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if key:
            values[key] = value
    return values


def influx_config_value(config_key: str, *explicit_env_names: str) -> str | None:
    explicit_value = env_value(*explicit_env_names)
    if explicit_value:
        return explicit_value

    direct_value = env_value(config_key)
    if direct_value:
        return direct_value

    metrics_config = parse_solana_metrics_config()
    config_value = metrics_config.get(config_key)
    if config_value:
        return config_value

    return None


def require_influx_config_value(config_key: str, *explicit_env_names: str) -> str:
    value = influx_config_value(config_key, *explicit_env_names)
    if value is None:
        options = ", ".join([*explicit_env_names, config_key, "SOLANA_METRICS_CONFIG"])
        raise ValueError(f"missing required Influx setting; expected one of: {options}")
    return value


def normalize_influx_endpoint(
    host_value: str,
    port_value: str | None,
    scheme: str,
) -> tuple[str, str, str]:
    if "://" not in host_value:
        if not port_value:
            raise ValueError("missing required Influx setting; expected port")
        return host_value, port_value, scheme

    parsed = urllib.parse.urlparse(host_value)
    if not parsed.hostname:
        raise ValueError(f"invalid Influx host URL: {host_value}")

    endpoint_scheme = parsed.scheme or scheme
    endpoint_port = parsed.port
    if endpoint_port is None and port_value:
        endpoint_port = int(port_value)
    if endpoint_port is None:
        endpoint_port = 443 if endpoint_scheme == "https" else 80

    return parsed.hostname, str(endpoint_port), endpoint_scheme


def quote_influx_identifier(identifier: str) -> str:
    escaped = identifier.replace("\\", "\\\\").replace('"', '\"')
    return f'"{escaped}"'


def hours_to_influx_duration(hours: float) -> str:
    if hours <= 0:
        raise ValueError("--hours must be greater than zero")
    if hours.is_integer():
        return f"{int(hours)}h"
    return f"{round(hours * 3600)}s"


def validate_influx_duration(duration: str, arg_name: str) -> str:
    if not INFLUX_DURATION_PATTERN.fullmatch(duration):
        raise ValueError(
            f"{arg_name} must be an Influx duration like 30s, 1m, 6h, or 1d"
        )
    return duration


def build_xdp_influx_query(
    database: str,
    retention_policy: str,
    measurement: str,
    duration: str,
    interval: str,
) -> str:
    return (
        'SELECT mean("num_nodes") AS "mean_num_nodes" '
        f"FROM {quote_influx_identifier(database)}."
        f"{quote_influx_identifier(retention_policy)}."
        f"{quote_influx_identifier(measurement)} "
        f"WHERE time > now() - {duration} "
        "AND time < now() AND \"is_xdp\"='true' "
        f'GROUP BY time({interval}), "host_id" FILL(null)'
    )


def build_dashboard_xdp_influx_query(
    database: str,
    retention_policy: str,
    measurement: str,
    duration: str,
) -> str:
    return (
        'SELECT last("slot") '
        f"FROM {quote_influx_identifier(database)}."
        f"{quote_influx_identifier(retention_policy)}."
        f"{quote_influx_identifier(measurement)} "
        f"WHERE time > now() - {duration} "
        'GROUP BY "host_id", "is_xdp"'
    )


def build_dashboard_packet_influx_query(
    database: str,
    retention_policy: str,
    measurement: str,
    duration: str,
    interval: str,
) -> str:
    return (
        'SELECT sum("total_packets") AS "packets_sent" '
        f"FROM {quote_influx_identifier(database)}."
        f"{quote_influx_identifier(retention_policy)}."
        f"{quote_influx_identifier(measurement)} "
        f"WHERE time > now() - {duration} "
        "AND time < now() "
        "AND (\"is_xdp\"='true' OR \"is_xdp\"='false') "
        f'GROUP BY time({interval}), "host_id", "is_xdp" FILL(0)'
    )


def build_influx_request(
    url: str,
    database: str,
    query: str,
    username: str,
    password: str,
    auth_mode: str,
) -> urllib.request.Request:
    if auth_mode == "basic":
        body = urllib.parse.urlencode({"db": database, "q": query}).encode()
        auth = base64.b64encode(f"{username}:{password}".encode()).decode()
        return urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

    if auth_mode == "query-params":
        params = urllib.parse.urlencode(
            {"db": database, "q": query, "u": username, "p": password}
        )
        return urllib.request.Request(f"{url}?{params}", method="GET")

    if auth_mode == "body-params":
        body = urllib.parse.urlencode(
            {"db": database, "q": query, "u": username, "p": password}
        ).encode()
        return urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    raise ValueError(f"unsupported Influx auth mode: {auth_mode}")


def query_influx_once(
    request: urllib.request.Request,
    timeout: int,
    scheme: str,
) -> dict[str, Any]:
    context = ssl.create_default_context() if scheme == "https" else None
    with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
        payload: dict[str, Any] = json.loads(response.read().decode())

    for result in payload.get("results", []):
        if isinstance(result, dict) and result.get("error"):
            raise RuntimeError(f"Influx query failed: {result['error']}")

    return payload


def query_influx(
    query: str,
    database: str,
    timeout: int,
    scheme: str,
    auth_mode: str,
) -> dict[str, Any]:
    host_value = require_influx_config_value(
        "host", "INFLUXDB_HOST", "INFLUX_HOST"
    )
    port_value = influx_config_value("port", "INFLUXDB_PORT", "INFLUX_PORT")
    username = require_influx_config_value(
        "u", "INFLUXDB_USERNAME", "INFLUX_USER"
    )
    password = require_influx_config_value(
        "p", "INFLUXDB_PASSWORD", "INFLUX_PASSWORD"
    )
    host, port, scheme = normalize_influx_endpoint(host_value, port_value, scheme)
    url = f"{scheme}://{host}:{port}/query"
    auth_modes = (
        ["basic", "query-params", "body-params"]
        if auth_mode == "auto"
        else [auth_mode]
    )

    failures: list[str] = []
    for mode in auth_modes:
        request = build_influx_request(url, database, query, username, password, mode)
        try:
            return query_influx_once(request, timeout, scheme)
        except urllib.error.HTTPError as error:
            details = error.read().decode(errors="replace")
            failures.append(f"{mode}: HTTP {error.code}: {details}")
            if error.code != 401 or auth_mode != "auto":
                break
        except urllib.error.URLError as error:
            failures.append(f"{mode}: {error.reason}")
            break

    raise RuntimeError("Influx query failed; " + "; ".join(failures))


def influx_series(payload: dict[str, Any]) -> list[dict[str, Any]]:
    series: list[dict[str, Any]] = []
    for result in payload.get("results", []):
        if isinstance(result, dict):
            result_series = result.get("series", [])
            if isinstance(result_series, list):
                series.extend(item for item in result_series if isinstance(item, dict))
    return series


def host_ids_from_influx_payload(payload: dict[str, Any]) -> set[str]:
    host_ids: set[str] = set()
    for series in influx_series(payload):
        tags = series.get("tags", {})
        if not isinstance(tags, dict):
            continue
        host_id = tags.get("host_id")
        if isinstance(host_id, str) and host_id.strip():
            host_ids.add(host_id.strip())
    return host_ids


def host_ids_by_is_xdp_tag_from_influx_payload(
    payload: dict[str, Any],
) -> tuple[set[str], set[str]]:
    xdp_host_ids: set[str] = set()
    non_xdp_host_ids: set[str] = set()

    for series in influx_series(payload):
        tags = series.get("tags", {})
        if not isinstance(tags, dict):
            continue

        host_id = tags.get("host_id")
        is_xdp = tags.get("is_xdp")
        if not isinstance(host_id, str) or not host_id.strip():
            continue
        if not isinstance(is_xdp, str):
            continue

        normalized_host_id = host_id.strip()
        normalized_is_xdp = is_xdp.strip().lower()
        if normalized_is_xdp == "true":
            xdp_host_ids.add(normalized_host_id)
        elif normalized_is_xdp == "false":
            non_xdp_host_ids.add(normalized_host_id)

    return xdp_host_ids, non_xdp_host_ids


def packet_totals_by_host_from_influx_payload(
    payload: dict[str, Any],
) -> dict[str, dict[str, float]]:
    packet_totals_by_host: dict[str, dict[str, float]] = {}

    for series in influx_series(payload):
        tags = series.get("tags", {})
        if not isinstance(tags, dict):
            continue

        host_id = tags.get("host_id")
        is_xdp = tags.get("is_xdp")
        if not isinstance(host_id, str) or not host_id.strip():
            continue
        if not isinstance(is_xdp, str):
            continue

        normalized_host_id = host_id.strip()
        normalized_is_xdp = is_xdp.strip().lower()
        if normalized_is_xdp not in {"true", "false"}:
            continue

        columns = series.get("columns", [])
        values = series.get("values", [])
        if not isinstance(columns, list) or not isinstance(values, list):
            continue
        try:
            value_index = columns.index("packets_sent")
        except ValueError:
            continue

        host_totals = packet_totals_by_host.setdefault(
            normalized_host_id,
            {"true": 0.0, "false": 0.0},
        )
        for value_row in values:
            if not isinstance(value_row, list) or value_index >= len(value_row):
                continue

            packets_sent = value_row[value_index]
            if packets_sent is None:
                continue
            if not isinstance(packets_sent, (int, float)) or isinstance(
                packets_sent,
                bool,
            ):
                raise ValueError(f"non-numeric packets_sent value: {packets_sent!r}")

            host_totals[normalized_is_xdp] += float(packets_sent)

    return packet_totals_by_host


def write_dashboard_influx_payload_to_csv(
    payload: dict[str, Any],
    csv_path: Path,
    measurement: str,
) -> int:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    value_column = f"{measurement}.last_slot"

    with csv_path.open("w", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["time", "host_id", "is_xdp", value_column],
        )
        writer.writeheader()

        for series in influx_series(payload):
            tags = series.get("tags", {})
            host_id = tags.get("host_id") if isinstance(tags, dict) else None
            is_xdp = tags.get("is_xdp") if isinstance(tags, dict) else None
            if not isinstance(host_id, str) or not host_id.strip():
                continue
            if not isinstance(is_xdp, str) or not is_xdp.strip():
                continue

            columns = series.get("columns", [])
            values = series.get("values", [])
            if not isinstance(columns, list) or not isinstance(values, list):
                continue
            try:
                time_index = columns.index("time")
                value_index = columns.index("last")
            except ValueError:
                continue

            for value_row in values:
                if not isinstance(value_row, list):
                    continue
                last_slot = (
                    value_row[value_index] if value_index < len(value_row) else None
                )
                writer.writerow(
                    {
                        "time": value_row[time_index] if time_index < len(value_row) else "",
                        "host_id": host_id.strip(),
                        "is_xdp": is_xdp.strip(),
                        value_column: "" if last_slot is None else last_slot,
                    }
                )
                rows_written += 1

    return rows_written


def write_influx_payload_to_csv(
    payload: dict[str, Any],
    csv_path: Path,
    measurement: str,
) -> int:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    value_column = f"{measurement}.mean_num_nodes"

    with csv_path.open("w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["time", "host_id", value_column])
        writer.writeheader()

        for series in influx_series(payload):
            tags = series.get("tags", {})
            host_id = tags.get("host_id") if isinstance(tags, dict) else None
            if not isinstance(host_id, str) or not host_id.strip():
                continue

            columns = series.get("columns", [])
            values = series.get("values", [])
            if not isinstance(columns, list) or not isinstance(values, list):
                continue
            try:
                time_index = columns.index("time")
                value_index = columns.index("mean_num_nodes")
            except ValueError:
                continue

            for value_row in values:
                if not isinstance(value_row, list):
                    continue
                mean_num_nodes = (
                    value_row[value_index] if value_index < len(value_row) else None
                )
                writer.writerow(
                    {
                        "time": value_row[time_index] if time_index < len(value_row) else "",
                        "host_id": host_id.strip(),
                        value_column: "" if mean_num_nodes is None else mean_num_nodes,
                    }
                )
                rows_written += 1

    return rows_written


def read_unique_host_ids(csv_path: Path, host_column: str) -> set[str]:
    with csv_path.open(newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            raise ValueError(f"{csv_path} has no header row")
        if host_column not in reader.fieldnames:
            columns = ", ".join(reader.fieldnames)
            raise ValueError(
                f"{csv_path} does not contain column {host_column!r}; found: {columns}"
            )

        filter_is_xdp = "is_xdp" in reader.fieldnames
        host_ids: set[str] = set()
        for row in reader:
            if filter_is_xdp and (row.get("is_xdp") or "").strip().lower() != "true":
                continue

            host_id = (row.get(host_column) or "").strip()
            if host_id:
                host_ids.add(host_id)

        return host_ids


def read_host_ids_by_is_xdp_tag_from_csv(
    csv_path: Path,
    host_column: str,
) -> tuple[set[str], set[str]]:
    with csv_path.open(newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            raise ValueError(f"{csv_path} has no header row")

        required_columns = [host_column, "is_xdp"]
        missing_columns = [
            column for column in required_columns if column not in reader.fieldnames
        ]
        if missing_columns:
            columns = ", ".join(reader.fieldnames)
            missing = ", ".join(repr(column) for column in missing_columns)
            raise ValueError(
                f"{csv_path} does not contain required column(s) {missing}; "
                f"found: {columns}"
            )

        xdp_host_ids: set[str] = set()
        non_xdp_host_ids: set[str] = set()
        for row in reader:
            host_id = (row.get(host_column) or "").strip()
            is_xdp = (row.get("is_xdp") or "").strip().lower()
            if not host_id:
                continue
            if is_xdp == "true":
                xdp_host_ids.add(host_id)
            elif is_xdp == "false":
                non_xdp_host_ids.add(host_id)

        return xdp_host_ids, non_xdp_host_ids


def read_stake_data(
    stake_json_path: Path,
    identity_key: str,
    stake_key: str,
    total_stake_key: str,
) -> tuple[dict[str, int], dict[str, str], int]:
    with stake_json_path.open() as stake_file:
        stake_data: dict[str, Any] = json.load(stake_file)

    validators = stake_data.get("validators")
    if not isinstance(validators, list):
        raise ValueError(f"{stake_json_path} does not contain a validators list")

    stake_by_identity: dict[str, int] = {}
    versions_by_identity: dict[str, set[str]] = {}
    for validator in validators:
        if not isinstance(validator, dict):
            continue

        identity = validator.get(identity_key)
        if not isinstance(identity, str):
            continue

        stake = validator.get(stake_key, 0)
        if not isinstance(stake, int):
            raise ValueError(
                f"validator {identity!r} has non-integer {stake_key}: {stake!r}"
            )

        stake_by_identity[identity] = stake_by_identity.get(identity, 0) + stake
        version = validator.get("version")
        versions_by_identity.setdefault(identity, set()).add(
            version if isinstance(version, str) else "unknown"
        )

    total_stake = stake_data.get(total_stake_key)
    if total_stake is None:
        total_stake = sum(stake_by_identity.values())
    if not isinstance(total_stake, int):
        raise ValueError(
            f"{stake_json_path} has non-integer {total_stake_key}: {total_stake!r}"
        )

    version_by_identity = {
        identity: ";".join(sorted(versions))
        for identity, versions in versions_by_identity.items()
    }

    return stake_by_identity, version_by_identity, total_stake


def format_csv_row(values: list[str]) -> str:
    output = io.StringIO()
    csv.writer(output, lineterminator="").writerow(values)
    return output.getvalue()


def html_to_text(raw_html: str) -> str:
    without_tags = re.sub(r"<[^>]+>", "", raw_html)
    return " ".join(html.unescape(without_tags).split())


def fetch_url_text(
    url: str,
    timeout: int,
    headers: dict[str, str] | None = None,
) -> str:
    request_headers = {"User-Agent": "xdp-stake-coverage/1.0"}
    if headers is not None:
        request_headers.update(headers)
    request = urllib.request.Request(url, headers=request_headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode(errors="replace")


def validator_names_from_validators_app_html(
    page_html: str,
    host_ids: set[str],
) -> dict[str, str]:
    validator_names: dict[str, str] = {}

    for account, raw_name in VALIDATORS_APP_NAME_LINK_PATTERN.findall(page_html):
        account = urllib.parse.unquote(html.unescape(account)).strip()
        if account not in host_ids:
            continue

        name = html_to_text(raw_name)
        if name:
            validator_names[account] = name

    return validator_names


def fetch_validator_names_from_validators_app_api(
    network: str,
    host_ids: set[str],
    token: str | None,
    timeout: int,
) -> dict[str, str]:
    if not token or not host_ids:
        return {}

    params = urllib.parse.urlencode({"active_only": "false", "limit": "9999"})
    url = f"{VALIDATORS_APP_BASE_URL}/api/v1/validators/{network}.json?{params}"
    try:
        payload = json.loads(
            fetch_url_text(
                url,
                timeout,
                headers={"Accept": "application/json", "Token": token},
            )
        )
    except (
        json.JSONDecodeError,
        TimeoutError,
        urllib.error.HTTPError,
        urllib.error.URLError,
    ):
        return {}

    if isinstance(payload, dict):
        validators = payload.get("validators", [])
    else:
        validators = payload
    if not isinstance(validators, list):
        return {}

    validator_names: dict[str, str] = {}
    for validator in validators:
        if not isinstance(validator, dict):
            continue

        account = validator.get("account")
        name = validator.get("name")
        if not isinstance(account, str) or account not in host_ids:
            continue
        if isinstance(name, str) and name.strip():
            validator_names[account] = name.strip()

    return validator_names


def fetch_validator_names_from_validators_app_pages(
    network: str,
    host_ids: set[str],
    timeout: int,
) -> dict[str, str]:
    validator_names: dict[str, str] = {}
    if not host_ids:
        return validator_names

    page_count = max(1, (len(host_ids) + 24) // 25 + 2)
    for page_number in range(1, page_count + 1):
        params = urllib.parse.urlencode(
            {
                "locale": "en",
                "network": network,
                "order": "stake",
                "page": page_number,
            }
        )
        url = f"{VALIDATORS_APP_BASE_URL}/validators?{params}"
        try:
            page_html = fetch_url_text(url, timeout)
        except (TimeoutError, urllib.error.HTTPError, urllib.error.URLError):
            break

        validator_names.update(
            validator_names_from_validators_app_html(
                page_html,
                host_ids - validator_names.keys(),
            )
        )
        if host_ids <= validator_names.keys():
            return validator_names

    for host_id in sorted(host_ids - validator_names.keys()):
        params = urllib.parse.urlencode(
            {"locale": "en", "network": network, "q": host_id}
        )
        url = f"{VALIDATORS_APP_BASE_URL}/validators?{params}"
        try:
            page_html = fetch_url_text(url, timeout)
        except urllib.error.HTTPError:
            continue
        except (TimeoutError, urllib.error.URLError):
            return validator_names

        validator_names.update(
            validator_names_from_validators_app_html(page_html, {host_id})
        )

    return validator_names


def validator_info_object_names(value: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for key in ("name", "validatorName", "validator_name"):
        name = value.get(key)
        if isinstance(name, str) and name.strip():
            names.append(name.strip())

    for key in ("info", "validatorInfo", "validator_info", "configData", "config_data"):
        nested = value.get(key)
        if not isinstance(nested, dict):
            continue
        for nested_name in validator_info_object_names(nested):
            names.append(nested_name)

    return names


def collect_validator_info_names(
    value: Any,
    host_ids: set[str],
    validator_names: dict[str, str],
) -> None:
    if isinstance(value, dict):
        local_host_ids = {
            item for item in value.values() if isinstance(item, str) and item in host_ids
        }
        local_names = validator_info_object_names(value)
        if local_host_ids and local_names:
            for host_id in local_host_ids:
                validator_names.setdefault(host_id, local_names[0])

        for child in value.values():
            collect_validator_info_names(child, host_ids, validator_names)
    elif isinstance(value, list):
        for child in value:
            collect_validator_info_names(child, host_ids, validator_names)


def fetch_validator_names_from_solana_validator_info(
    network: str,
    host_ids: set[str],
    solana_bin: str,
    timeout: int,
) -> dict[str, str]:
    if not host_ids:
        return {}

    solana_url = NETWORK_FILES[network]["solana_url"]
    command = [
        solana_bin,
        "-u",
        solana_url,
        "validator-info",
        "get",
        "--output",
        "json",
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}

    if result.returncode != 0:
        return {}

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}

    validator_names: dict[str, str] = {}
    collect_validator_info_names(payload, host_ids, validator_names)
    return validator_names


def lookup_validator_names(
    network: str,
    host_ids: set[str],
    validators_app_token: str | None,
    validators_app_timeout: int,
    solana_bin: str,
    solana_timeout: int,
) -> dict[str, str]:
    validator_names = fetch_validator_names_from_validators_app_api(
        network,
        host_ids,
        validators_app_token,
        validators_app_timeout,
    )

    missing_host_ids = host_ids - validator_names.keys()
    if missing_host_ids:
        validator_names.update(
            fetch_validator_names_from_validators_app_pages(
                network,
                missing_host_ids,
                validators_app_timeout,
            )
        )

    missing_host_ids = host_ids - validator_names.keys()
    if missing_host_ids:
        validator_names.update(
            fetch_validator_names_from_solana_validator_info(
                network,
                missing_host_ids,
                solana_bin,
                solana_timeout,
            )
        )

    return {
        host_id: name
        for host_id, name in validator_names.items()
        if host_id in host_ids and name
    }


def format_fraction(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "n/a"

    fraction = Fraction(numerator, denominator)
    return f"{fraction.numerator}/{fraction.denominator}"


def format_packet_fraction(numerator: float, denominator: float) -> str:
    if denominator == 0:
        return "n/a"
    return f"{numerator / denominator:.6f}"


def packet_totals_for_host_ids(
    packet_totals_by_host: dict[str, dict[str, float]],
    host_ids: set[str],
) -> tuple[float, float]:
    xdp_packets = 0.0
    non_xdp_packets = 0.0

    for host_id in host_ids:
        host_totals = packet_totals_by_host.get(host_id)
        if host_totals is None:
            continue

        xdp_packets += host_totals.get("true", 0.0)
        non_xdp_packets += host_totals.get("false", 0.0)

    return xdp_packets, non_xdp_packets


def print_packet_fraction_scope(
    scope_name: str,
    scope_host_ids: set[str],
    packet_totals_by_host: dict[str, dict[str, float]],
) -> None:
    xdp_packets, non_xdp_packets = packet_totals_for_host_ids(
        packet_totals_by_host,
        scope_host_ids,
    )
    total_packets = xdp_packets + non_xdp_packets

    print(
        ",".join(
            [
                scope_name,
                format_packet_fraction(xdp_packets, total_packets),
                format_packet_fraction(non_xdp_packets, total_packets),
            ]
        )
    )


def stake_percent_for_host(
    host_id: str,
    stake_by_identity: dict[str, int],
    total_stake: int,
) -> float:
    return stake_by_identity.get(host_id, 0) / total_stake * 100 if total_stake else 0.0


def is_xdp_states_for_host(
    host_id: str,
    xdp_true_host_ids: set[str],
    xdp_false_host_ids: set[str],
) -> str:
    states = []
    if host_id in xdp_true_host_ids:
        states.append("true")
    if host_id in xdp_false_host_ids:
        states.append("false")
    return ";".join(states) if states else "none"


def print_top_staked_xdp_status_report(
    all_staked_host_ids_by_stake: list[str],
    stake_by_identity: dict[str, int],
    total_stake: int,
    xdp_true_host_ids: set[str],
    xdp_false_host_ids: set[str],
    validator_names_by_identity: dict[str, str] | None = None,
) -> None:
    metric_reporting_host_ids = xdp_true_host_ids | xdp_false_host_ids
    validator_names = validator_names_by_identity or {}

    print("Top staked validator XDP metric status:")
    for top_count in TOP_STAKED_XDP_STATUS_COUNTS:
        top_host_ids = all_staked_host_ids_by_stake[:top_count]
        false_reporting_host_ids = [
            host_id for host_id in top_host_ids if host_id in xdp_false_host_ids
        ]
        non_reporting_host_ids = [
            host_id for host_id in top_host_ids if host_id not in metric_reporting_host_ids
        ]

        print()
        print(
            f"Top {top_count} staked validators explicitly reporting "
            f"is_xdp=false ({len(false_reporting_host_ids)}):"
        )
        print("pubkey,validator_name,stake_percent_of_total,is_xdp_states")
        for host_id in false_reporting_host_ids:
            host_stake_percent = stake_percent_for_host(
                host_id,
                stake_by_identity,
                total_stake,
            )
            is_xdp_states = is_xdp_states_for_host(
                host_id,
                xdp_true_host_ids,
                xdp_false_host_ids,
            )
            print(
                format_csv_row(
                    [
                        host_id,
                        validator_names.get(host_id, "unknown"),
                        f"{host_stake_percent:.6f}%",
                        is_xdp_states,
                    ]
                )
            )

        print()
        print(
            f"Top {top_count} staked validators not reporting metrics "
            f"({len(non_reporting_host_ids)}):"
        )
        print("pubkey,validator_name,stake_percent_of_total")
        for host_id in non_reporting_host_ids:
            host_stake_percent = stake_percent_for_host(
                host_id,
                stake_by_identity,
                total_stake,
            )
            print(
                format_csv_row(
                    [
                        host_id,
                        validator_names.get(host_id, "unknown"),
                        f"{host_stake_percent:.6f}%",
                    ]
                )
            )

    top_true_count = max(TOP_STAKED_XDP_STATUS_COUNTS)
    true_reporting_host_ids = [
        host_id
        for host_id in all_staked_host_ids_by_stake[:top_true_count]
        if host_id in xdp_true_host_ids
    ]

    print()
    print(
        f"Top {top_true_count} staked validators explicitly reporting "
        f"is_xdp=true ({len(true_reporting_host_ids)}):"
    )
    print("pubkey,validator_name,stake_percent_of_total,is_xdp_states")
    for host_id in true_reporting_host_ids:
        host_stake_percent = stake_percent_for_host(
            host_id,
            stake_by_identity,
            total_stake,
        )
        is_xdp_states = is_xdp_states_for_host(
            host_id,
            xdp_true_host_ids,
            xdp_false_host_ids,
        )
        print(
            format_csv_row(
                [
                    host_id,
                    validator_names.get(host_id, "unknown"),
                    f"{host_stake_percent:.6f}%",
                    is_xdp_states,
                ]
            )
        )


def refresh_stake_json(
    network: str,
    stake_json_path: Path,
    solana_bin: str,
    timeout: int,
) -> None:
    stake_json_path.parent.mkdir(parents=True, exist_ok=True)
    solana_url = NETWORK_FILES[network]["solana_url"]
    command = [
        solana_bin,
        "-u",
        solana_url,
        "validators",
        "--output",
        "json",
    ]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except FileNotFoundError as error:
        raise RuntimeError(f"{solana_bin!r} was not found in PATH") from error
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            f"{' '.join(command)} timed out after {timeout} seconds"
        ) from error

    if result.returncode != 0:
        raise RuntimeError(
            "failed to refresh validator stake JSON with "
            f"{' '.join(command)}:\n{result.stderr.strip()}"
        )

    try:
        json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"{' '.join(command)} did not return valid JSON: {error}"
        ) from error

    stake_json_path.write_text(result.stdout)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Calculate stake coverage for validators reporting is_xdp=true in Influx."
        )
    )
    parser.add_argument(
        "network",
        nargs="?",
        choices=sorted(NETWORK_FILES),
        default="testnet",
        help="network file set to use; defaults to testnet",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="read XDP host IDs from this CSV instead of querying Influx",
    )
    parser.add_argument(
        "--xdp-source",
        choices=(XDP_SOURCE_RETRANSMIT_STAGE, XDP_SOURCE_DASHBOARD),
        default=None,
        help=(
            "Influx query source when --csv is not supplied; defaults to "
            "retransmit-stage"
        ),
    )
    parser.add_argument(
        "--is-xdp-off",
        action="store_true",
        help=(
            "replace the final XDP host_id table with top-staked validators "
            "that only reported is_xdp=false"
        ),
    )
    parser.add_argument(
        "--packet-fractions",
        action="store_true",
        help=(
            "print packet fractions by leader is_xdp state for metric "
            "reporters, all validators, and top-staked validators"
        ),
    )
    parser.add_argument(
        "--top-staked-xdp-status",
        action="store_true",
        help=(
            "print top 10, 20, 50, and 100 staked validators that report "
            "is_xdp=false or do not report dashboard metrics"
        ),
    )
    parser.add_argument(
        "--no-validator-names",
        action="store_true",
        help="do not look up validator names for --top-staked-xdp-status",
    )
    parser.add_argument(
        "--validators-app-token",
        default=env_value("VALIDATORS_APP_TOKEN", "VALIDATORS_APP_API_TOKEN"),
        help=(
            "optional validators.app API token; without it, validator names "
            "are read from public validators.app pages"
        ),
    )
    parser.add_argument(
        "--validator-name-timeout",
        type=int,
        default=15,
        help="validators.app HTTP timeout in seconds for validator name lookup",
    )
    parser.add_argument(
        "--packet-measurement",
        default=DEFAULT_DASHBOARD_MEASUREMENT,
        help="Influx measurement to query for --packet-fractions",
    )
    parser.add_argument(
        "--xdp-csv-out",
        type=Path,
        default=None,
        help="where to write the Influx XDP query rows; defaults to the network CSV",
    )
    parser.add_argument(
        "--no-write-xdp-csv",
        action="store_true",
        help="do not write the fetched Influx rows to CSV",
    )
    parser.add_argument(
        "--stake-json",
        type=Path,
        default=None,
        help="validator stake JSON path; overrides the selected network default",
    )
    parser.add_argument(
        "--skip-stake-refresh",
        action="store_true",
        help="use the existing stake JSON instead of running solana validators",
    )
    parser.add_argument(
        "--solana-bin",
        default="solana",
        help="solana CLI binary to use when refreshing validator stake JSON",
    )
    parser.add_argument(
        "--solana-timeout",
        type=int,
        default=180,
        help="solana validators timeout in seconds",
    )
    parser.add_argument(
        "--csv-host-column",
        default="host_id",
        help="CSV column containing host identity pubkeys",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Influx .env file with db/u/p/host/port values",
    )
    parser.add_argument(
        "--influx-db",
        default=None,
        help=(
            "Influx database override; defaults to tds for testnet and "
            "mainnet-beta for mainnet"
        ),
    )
    parser.add_argument(
        "--retention-policy",
        default=DEFAULT_RETENTION_POLICY,
        help="Influx retention policy",
    )
    parser.add_argument(
        "--measurement",
        default=None,
        help="Influx measurement override; defaults based on --xdp-source",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=DEFAULT_INFLUX_HOURS,
        help="how many trailing hours of Influx data to query",
    )
    parser.add_argument(
        "--interval",
        default=DEFAULT_INFLUX_INTERVAL,
        help="Influx GROUP BY time interval",
    )
    parser.add_argument(
        "--influx-scheme",
        choices=("http", "https"),
        default=env_value("INFLUXDB_SCHEME", "INFLUX_SCHEME") or "https",
        help="Influx HTTP scheme; defaults to https",
    )
    parser.add_argument(
        "--influx-timeout",
        type=int,
        default=300,
        help="Influx HTTP timeout in seconds",
    )
    parser.add_argument(
        "--influx-auth",
        choices=("auto", "basic", "query-params", "body-params"),
        default="auto",
        help="Influx authentication placement; defaults to auto",
    )
    parser.add_argument(
        "--identity-key",
        default="identityPubkey",
        help="validator JSON key matching the CSV host ID",
    )
    parser.add_argument(
        "--stake-key",
        default="activatedStake",
        help="validator JSON stake field to sum",
    )
    parser.add_argument(
        "--total-stake-key",
        default="totalActiveStake",
        help="top-level JSON total stake field; falls back to sum of validators if absent",
    )
    parser.add_argument(
        "--top-staked-count",
        type=int,
        default=DEFAULT_TOP_STAKED_COUNT,
        help="how many of the highest-staked validators to summarize",
    )
    args = parser.parse_args()
    if args.top_staked_count <= 0:
        parser.error("--top-staked-count must be greater than zero")
    if args.validator_name_timeout <= 0:
        parser.error("--validator-name-timeout must be greater than zero")

    network_files = NETWORK_FILES[args.network]
    if (
        (args.is_xdp_off or args.top_staked_xdp_status)
        and args.csv is None
        and args.xdp_source is None
    ):
        args.xdp_source = XDP_SOURCE_DASHBOARD
    if args.xdp_source is None:
        args.xdp_source = XDP_SOURCE_RETRANSMIT_STAGE
    if (
        (args.is_xdp_off or args.top_staked_xdp_status)
        and args.csv is None
        and args.xdp_source != XDP_SOURCE_DASHBOARD
    ):
        parser.error(
            "--is-xdp-off and --top-staked-xdp-status require --xdp-source "
            f"{XDP_SOURCE_DASHBOARD} when querying Influx"
        )
    if args.measurement is None:
        args.measurement = (
            DEFAULT_DASHBOARD_MEASUREMENT
            if args.xdp_source == XDP_SOURCE_DASHBOARD
            else DEFAULT_MEASUREMENT
        )
    if args.xdp_csv_out is None:
        args.xdp_csv_out = network_files["csv"]
    if args.stake_json is None:
        args.stake_json = network_files["stake_json"]
    if args.influx_db is None:
        args.influx_db = network_files["influx_db"]
    args.duration = hours_to_influx_duration(args.hours)
    args.interval = validate_influx_duration(args.interval, "--interval")
    return args


def main() -> None:
    args = parse_args()

    csv_rows_written = None
    xdp_tag_host_ids = None
    loaded_env_file = False
    packet_only = (
        args.packet_fractions
        and not args.is_xdp_off
        and not args.top_staked_xdp_status
    )
    xdp_status_only = (
        args.top_staked_xdp_status
        and not args.is_xdp_off
        and not args.packet_fractions
    )
    host_ids: set[str] = set()
    xdp_source = "packet fraction report only"
    if packet_only:
        pass
    elif args.csv is not None:
        if args.is_xdp_off or args.top_staked_xdp_status:
            xdp_host_ids, non_xdp_host_ids = read_host_ids_by_is_xdp_tag_from_csv(
                args.csv,
                args.csv_host_column,
            )
            host_ids = xdp_host_ids
            xdp_tag_host_ids = (xdp_host_ids, non_xdp_host_ids)
            xdp_source = f"CSV file: {args.csv} (is_xdp tags)"
        else:
            host_ids = read_unique_host_ids(args.csv, args.csv_host_column)
            xdp_source = f"CSV file: {args.csv}"
    else:
        load_env_file(args.env_file)
        loaded_env_file = True
        if args.xdp_source == XDP_SOURCE_DASHBOARD:
            query = build_dashboard_xdp_influx_query(
                args.influx_db,
                args.retention_policy,
                args.measurement,
                args.duration,
            )
        else:
            query = build_xdp_influx_query(
                args.influx_db,
                args.retention_policy,
                args.measurement,
                args.duration,
                args.interval,
            )
        payload = query_influx(
            query,
            database=args.influx_db,
            timeout=args.influx_timeout,
            scheme=args.influx_scheme,
            auth_mode=args.influx_auth,
        )
        if args.xdp_source == XDP_SOURCE_DASHBOARD:
            xdp_host_ids, non_xdp_host_ids = host_ids_by_is_xdp_tag_from_influx_payload(
                payload
            )
            host_ids = xdp_host_ids
            xdp_tag_host_ids = (xdp_host_ids, non_xdp_host_ids)
            xdp_source = (
                f"Influx dashboard query: database={args.influx_db}, "
                f"measurement={args.measurement}, last={args.duration}, "
                "group_by=host_id,is_xdp"
            )
            if not args.no_write_xdp_csv:
                csv_rows_written = write_dashboard_influx_payload_to_csv(
                    payload,
                    args.xdp_csv_out,
                    args.measurement,
                )
        else:
            host_ids = host_ids_from_influx_payload(payload)
            xdp_source = (
                f"Influx: database={args.influx_db}, measurement={args.measurement}, "
                f"last={args.duration}, interval={args.interval}"
            )
            if not args.no_write_xdp_csv:
                csv_rows_written = write_influx_payload_to_csv(
                    payload,
                    args.xdp_csv_out,
                    args.measurement,
                )

    if not args.skip_stake_refresh:
        refresh_stake_json(
            args.network,
            args.stake_json,
            args.solana_bin,
            args.solana_timeout,
        )

    stake_by_identity, version_by_identity, total_stake = read_stake_data(
        args.stake_json,
        args.identity_key,
        args.stake_key,
        args.total_stake_key,
    )

    found_host_ids = host_ids & stake_by_identity.keys()
    missing_host_ids = host_ids - stake_by_identity.keys()
    found_stake = sum(stake_by_identity[host_id] for host_id in found_host_ids)

    percent = (found_stake / total_stake * 100) if total_stake else 0.0
    all_staked_host_ids_by_stake = sorted(
        stake_by_identity,
        key=lambda host_id: (-stake_by_identity[host_id], host_id),
    )
    top_staked_host_ids = all_staked_host_ids_by_stake[: args.top_staked_count]
    top_staked_host_id_set = set(top_staked_host_ids)
    top_staked_xdp_host_ids = top_staked_host_id_set & host_ids
    top_staked_count = len(top_staked_host_ids)
    top_staked_xdp_count = len(top_staked_xdp_host_ids)
    top_staked_total_stake = sum(
        stake_by_identity[host_id] for host_id in top_staked_host_ids
    )
    top_staked_xdp_stake = sum(
        stake_by_identity[host_id] for host_id in top_staked_xdp_host_ids
    )
    top_staked_node_percent = (
        top_staked_xdp_count / top_staked_count * 100 if top_staked_count else 0.0
    )
    top_staked_stake_percent = (
        top_staked_xdp_stake / top_staked_total_stake * 100
        if top_staked_total_stake
        else 0.0
    )
    xdp_off_host_ids: set[str] = set()
    if xdp_tag_host_ids is not None:
        xdp_true_host_ids, xdp_false_host_ids = xdp_tag_host_ids
        xdp_off_host_ids = xdp_false_host_ids - xdp_true_host_ids
    top_staked_xdp_off_host_ids = [
        host_id for host_id in top_staked_host_ids if host_id in xdp_off_host_ids
    ]
    top_staked_xdp_off_stake = sum(
        stake_by_identity[host_id] for host_id in top_staked_xdp_off_host_ids
    )
    top_staked_xdp_off_stake_percent = (
        top_staked_xdp_off_stake / total_stake * 100 if total_stake else 0.0
    )
    validator_names_by_identity: dict[str, str] = {}
    if args.top_staked_xdp_status and not args.no_validator_names:
        validator_name_host_ids = set(
            all_staked_host_ids_by_stake[: max(TOP_STAKED_XDP_STATUS_COUNTS)]
        )
        validator_names_by_identity = lookup_validator_names(
            args.network,
            validator_name_host_ids,
            args.validators_app_token,
            args.validator_name_timeout,
            args.solana_bin,
            args.solana_timeout,
        )
    if xdp_status_only:
        if xdp_tag_host_ids is None:
            raise RuntimeError("top staked XDP status report requires is_xdp tag data")
        xdp_true_host_ids, xdp_false_host_ids = xdp_tag_host_ids
        print(f"Network: {args.network}")
        print(xdp_source)
        print(f"Stake JSON file: {args.stake_json}")
        print_top_staked_xdp_status_report(
            all_staked_host_ids_by_stake,
            stake_by_identity,
            total_stake,
            xdp_true_host_ids,
            xdp_false_host_ids,
            validator_names_by_identity,
        )
        return

    packet_totals_by_host = None
    packet_source = None
    if args.packet_fractions:
        if not loaded_env_file:
            load_env_file(args.env_file)
            loaded_env_file = True

        packet_query = build_dashboard_packet_influx_query(
            args.influx_db,
            args.retention_policy,
            args.packet_measurement,
            args.duration,
            args.interval,
        )
        packet_payload = query_influx(
            packet_query,
            database=args.influx_db,
            timeout=args.influx_timeout,
            scheme=args.influx_scheme,
            auth_mode=args.influx_auth,
        )
        packet_totals_by_host = packet_totals_by_host_from_influx_payload(
            packet_payload
        )
        packet_source = (
            f"Influx packet query: database={args.influx_db}, "
            f"measurement={args.packet_measurement}, field=total_packets, "
            f"last={args.duration}, interval={args.interval}, "
            "group_by=time,host_id,is_xdp"
        )

    if packet_only:
        packet_reporting_host_ids = (
            set(packet_totals_by_host) if packet_totals_by_host is not None else set()
        )
        print(f"Network: {args.network}")
        print(f"Stake JSON file: {args.stake_json}")
        if packet_source is not None:
            print(packet_source)
        print("Packet fractions by leader XDP state:")
        print("scope,leader_xdp_on_packet_fraction,leader_xdp_off_packet_fraction")
        print_packet_fraction_scope(
            "metric_reporting_validators",
            packet_reporting_host_ids,
            packet_totals_by_host or {},
        )
        print_packet_fraction_scope(
            "all_validators",
            set(stake_by_identity),
            packet_totals_by_host or {},
        )
        packet_top_staked_counts = [10, 20, 50, top_staked_count]
        seen_packet_top_staked_counts: set[int] = set()
        for packet_top_staked_count in packet_top_staked_counts:
            if packet_top_staked_count in seen_packet_top_staked_counts:
                continue
            seen_packet_top_staked_counts.add(packet_top_staked_count)
            packet_top_staked_host_ids = set(
                all_staked_host_ids_by_stake[:packet_top_staked_count]
            )
            print_packet_fraction_scope(
                f"top_{packet_top_staked_count}_staked_validators",
                packet_top_staked_host_ids,
                packet_totals_by_host or {},
            )
        return

    print(f"Network: {args.network}")
    print(xdp_source)
    if csv_rows_written is not None:
        print(f"Wrote XDP query CSV: {args.xdp_csv_out} ({csv_rows_written} rows)")
    print(f"Stake JSON file: {args.stake_json}")
    print(f"Unique XDP host_id count: {len(host_ids)}")
    if xdp_tag_host_ids is not None:
        xdp_tag_source = "CSV" if args.csv is not None else "Influx"
        xdp_true_host_ids, xdp_false_host_ids = xdp_tag_host_ids
        classified_host_ids = xdp_true_host_ids | xdp_false_host_ids
        false_only_host_ids = xdp_off_host_ids
        mixed_host_ids = xdp_true_host_ids & xdp_false_host_ids
        classified_percent = (
            len(xdp_true_host_ids) / len(classified_host_ids) * 100
            if classified_host_ids
            else 0.0
        )
        print(f"{xdp_tag_source} classified host_id count: {len(classified_host_ids)}")
        print(f"{xdp_tag_source} non-XDP-only host_id count: {len(false_only_host_ids)}")
        print(f"{xdp_tag_source} host IDs with both is_xdp states: {len(mixed_host_ids)}")
        print(
            f"{xdp_tag_source} XDP host_id fraction: "
            f"{format_fraction(len(xdp_true_host_ids), len(classified_host_ids))}"
        )
        print(f"{xdp_tag_source} XDP host_id percent: {classified_percent:.6f}%")
    print(f"Host IDs found in stake JSON: {len(found_host_ids)}")
    print(f"Host IDs missing from stake JSON: {len(missing_host_ids)}")
    print(f"Stake for found host IDs: {found_stake}")
    print(f"Total stake in JSON ({args.total_stake_key}): {total_stake}")
    print(
        "Stake fraction represented by XDP host IDs: "
        f"{format_fraction(found_stake, total_stake)}"
    )
    print(f"Stake percent represented by XDP host IDs: {percent:.6f}%")
    print()
    print(f"Top {top_staked_count} staked nodes:")
    print(f"XDP nodes in top {top_staked_count}: {top_staked_xdp_count}")
    print(
        f"Percent of top {top_staked_count} staked nodes running XDP: "
        f"{top_staked_node_percent:.6f}%"
    )
    print(f"Stake for XDP nodes in top {top_staked_count}: {top_staked_xdp_stake}")
    print(f"Total stake in top {top_staked_count}: {top_staked_total_stake}")
    print(
        f"Stake fraction represented by XDP nodes within top {top_staked_count}: "
        f"{format_fraction(top_staked_xdp_stake, top_staked_total_stake)}"
    )
    print(
        f"Stake percent represented by XDP nodes within top {top_staked_count}: "
        f"{top_staked_stake_percent:.6f}%"
    )
    print()
    if packet_totals_by_host is not None and packet_source is not None:
        packet_reporting_host_ids = set(packet_totals_by_host)
        print("Packet fractions by leader XDP state:")
        print(packet_source)
        print("scope,leader_xdp_on_packet_fraction,leader_xdp_off_packet_fraction")
        print_packet_fraction_scope(
            "metric_reporting_validators",
            packet_reporting_host_ids,
            packet_totals_by_host,
        )
        print_packet_fraction_scope(
            "all_validators",
            set(stake_by_identity),
            packet_totals_by_host,
        )
        packet_top_staked_counts = [10, 20, 50, top_staked_count]
        seen_packet_top_staked_counts: set[int] = set()
        for packet_top_staked_count in packet_top_staked_counts:
            if packet_top_staked_count in seen_packet_top_staked_counts:
                continue
            seen_packet_top_staked_counts.add(packet_top_staked_count)
            packet_top_staked_host_ids = set(
                all_staked_host_ids_by_stake[:packet_top_staked_count]
            )
            print_packet_fraction_scope(
                f"top_{packet_top_staked_count}_staked_validators",
                packet_top_staked_host_ids,
                packet_totals_by_host,
            )
        print()
    if args.top_staked_xdp_status:
        if xdp_tag_host_ids is None:
            raise RuntimeError("top staked XDP status report requires is_xdp tag data")
        xdp_true_host_ids, xdp_false_host_ids = xdp_tag_host_ids
        print_top_staked_xdp_status_report(
            all_staked_host_ids_by_stake,
            stake_by_identity,
            total_stake,
            xdp_true_host_ids,
            xdp_false_host_ids,
            validator_names_by_identity,
        )
        print()
    if args.is_xdp_off:
        print(f"Top {top_staked_count} staked validators with XDP off:")
        print(
            f"XDP-off validators in top {top_staked_count}: "
            f"{len(top_staked_xdp_off_host_ids)}"
        )
        print(
            f"Stake percent represented by XDP-off validators in top "
            f"{top_staked_count}: {top_staked_xdp_off_stake_percent:.6f}%"
        )
        print("pubkey,stake_percent_of_total")
        for host_id in top_staked_xdp_off_host_ids:
            host_stake_percent = (
                stake_by_identity[host_id] / total_stake * 100 if total_stake else 0.0
            )
            print(f"{host_id},{host_stake_percent:.6f}%")
    else:
        print("XDP host_id stakes:")
        print("host_id,stake_percent_of_total,version,status")
        for host_id in sorted(
            host_ids,
            key=lambda host_id: (-stake_by_identity.get(host_id, 0), host_id),
        ):
            if host_id in stake_by_identity:
                host_stake_percent = (
                    stake_by_identity[host_id] / total_stake * 100
                    if total_stake
                    else 0.0
                )
                version = version_by_identity.get(host_id, "unknown")
                print(f"{host_id},{host_stake_percent:.6f}%,{version},found")
            else:
                print(f"{host_id},0.000000%,unknown,missing")


if __name__ == "__main__":
    main()
