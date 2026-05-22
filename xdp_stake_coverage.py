#!/usr/bin/env python3
"""Report how much validator stake is represented by XDP validators."""

from __future__ import annotations

import argparse
import base64
import csv
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
DEFAULT_INFLUX_INTERVAL = "1m"
DEFAULT_INFLUX_HOURS = 12
INFLUX_DURATION_PATTERN = re.compile(r"^[1-9][0-9]*(?:u|us|ms|s|m|h|d|w)$")


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

        return {
            host_id.strip()
            for row in reader
            if (host_id := (row.get(host_column) or "").strip())
        }


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


def format_fraction(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "n/a"

    fraction = Fraction(numerator, denominator)
    return f"{fraction.numerator}/{fraction.denominator}"


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
        default=DEFAULT_MEASUREMENT,
        help="Influx measurement to query",
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
    args = parser.parse_args()
    network_files = NETWORK_FILES[args.network]
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
    if args.csv is not None:
        host_ids = read_unique_host_ids(args.csv, args.csv_host_column)
        xdp_source = f"CSV file: {args.csv}"
    else:
        load_env_file(args.env_file)
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

    print(f"Network: {args.network}")
    print(xdp_source)
    if csv_rows_written is not None:
        print(f"Wrote XDP query CSV: {args.xdp_csv_out} ({csv_rows_written} rows)")
    print(f"Stake JSON file: {args.stake_json}")
    print(f"Unique XDP host_id count: {len(host_ids)}")
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
    print("XDP host_id stakes:")
    print("host_id,stake_percent_of_total,version,status")
    for host_id in sorted(host_ids):
        if host_id in stake_by_identity:
            host_stake_percent = (
                stake_by_identity[host_id] / total_stake * 100 if total_stake else 0.0
            )
            version = version_by_identity.get(host_id, "unknown")
            print(f"{host_id},{host_stake_percent:.6f}%,{version},found")
        else:
            print(f"{host_id},0.000000%,unknown,missing")


if __name__ == "__main__":
    main()
