#!/usr/bin/env python3
"""Report how much validator stake is represented in an XDP host CSV."""

from __future__ import annotations

import argparse
import csv
import json
from fractions import Fraction
from pathlib import Path
from typing import Any


NETWORK_FILES = {
    "testnet": {
        "csv": Path("data/testnet-xdp-data.csv"),
        "stake_json": Path("data/testnet-stake.json"),
    },
    "mainnet": {
        "csv": Path("data/mainnet-xdp-data.csv"),
        "stake_json": Path("data/mainnet-stake.json"),
    },
}


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate the stake coverage of host IDs present in an XDP CSV."
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
        help="XDP CSV path; overrides the selected network default",
    )
    parser.add_argument(
        "--stake-json",
        type=Path,
        default=None,
        help="validator stake JSON path; overrides the selected network default",
    )
    parser.add_argument(
        "--csv-host-column",
        default="host_id",
        help="CSV column containing host identity pubkeys",
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
    if args.csv is None:
        args.csv = network_files["csv"]
    if args.stake_json is None:
        args.stake_json = network_files["stake_json"]
    return args


def main() -> None:
    args = parse_args()

    host_ids = read_unique_host_ids(args.csv, args.csv_host_column)
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
    print(f"CSV file: {args.csv}")
    print(f"Stake JSON file: {args.stake_json}")
    print(f"Unique host_id count in CSV: {len(host_ids)}")
    print(f"Host IDs found in stake JSON: {len(found_host_ids)}")
    print(f"Host IDs missing from stake JSON: {len(missing_host_ids)}")
    print(f"Stake for found host IDs: {found_stake}")
    print(f"Total stake in JSON ({args.total_stake_key}): {total_stake}")
    print(
        "Stake fraction represented by CSV host IDs: "
        f"{format_fraction(found_stake, total_stake)}"
    )
    print(f"Stake percent represented by CSV host IDs: {percent:.6f}%")
    print()
    print("CSV host_id stakes:")
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
