# XDP Stake Coverage

`xdp_stake_coverage.py` reports how much Solana validator stake is associated with validators reporting XDP status in metrics. It can query Influx directly, read a previously written CSV, refresh validator stake from the Solana CLI, and annotate top-staked status reports with validator names from validators.app.

## Requirements

- Python 3.10 or newer.
- `solana` CLI in `PATH` unless you use `--skip-stake-refresh`.
- Influx credentials when querying metrics directly.
- Network access for live Influx queries, stake refreshes, and validator name lookup.

The script supports `testnet` and `mainnet`. The default network is `testnet`.

## Quick Start

Default XDP stake coverage for testnet:

```bash
python3 xdp_stake_coverage.py
```

Mainnet XDP stake coverage:

```bash
python3 xdp_stake_coverage.py mainnet
```

Use existing local data instead of refreshing validator stake:

```bash
python3 xdp_stake_coverage.py mainnet --skip-stake-refresh
```

Read XDP data from a CSV instead of querying Influx:

```bash
python3 xdp_stake_coverage.py mainnet --csv data/mainnet-xdp-data.csv --skip-stake-refresh
```

Report top-staked validators that report `is_xdp=false`, do not report metrics, and the top 100 that report `is_xdp=true`:

```bash
python3 xdp_stake_coverage.py mainnet --top-staked-xdp-status
```

Print packet fractions by leader XDP state:

```bash
python3 xdp_stake_coverage.py mainnet --packet-fractions
```

## Influx Configuration

When `--csv` is not supplied, the script loads Influx settings from `--env-file` first, then the process environment. The default env file is `.env`.

Required settings can be provided with either the explicit environment names or through `SOLANA_METRICS_CONFIG`:

| Setting | Explicit environment names | `SOLANA_METRICS_CONFIG` key |
| --- | --- | --- |
| Host | `INFLUXDB_HOST`, `INFLUX_HOST` | `host` |
| Port | `INFLUXDB_PORT`, `INFLUX_PORT` | `port` |
| Username | `INFLUXDB_USERNAME`, `INFLUX_USER` | `u` |
| Password | `INFLUXDB_PASSWORD`, `INFLUX_PASSWORD` | `p` |
| Scheme | `INFLUXDB_SCHEME`, `INFLUX_SCHEME` | n/a |

`SOLANA_METRICS_CONFIG` values are comma-separated, for example:

```bash
SOLANA_METRICS_CONFIG="host=https://influx.example.com,u=user,p=password"
```

The default Influx databases are:

| Network | Influx database | Stake JSON | XDP CSV |
| --- | --- | --- | --- |
| `testnet` | `tds` | `data/testnet-stake.json` | `data/testnet-xdp-data.csv` |
| `mainnet` | `mainnet-beta` | `data/mainnet-stake.json` | `data/mainnet-xdp-data.csv` |

## Report Modes

### Default Stake Coverage

Without special report flags, the script:

1. Gets XDP-reporting validator host IDs from Influx or `--csv`.
2. Refreshes validator stake JSON with `solana validators --output json`, unless `--skip-stake-refresh` is set.
3. Prints network-level stake coverage for XDP host IDs.
4. Prints top-staked node coverage.
5. Prints a final `XDP host_id stakes` CSV table.

The default Influx XDP source is `retransmit-stage`, which queries `is_xdp=true` host IDs.

### Dashboard XDP Status

`--is-xdp-off` and `--top-staked-xdp-status` use the dashboard measurement by default when querying Influx:

```text
broadcast-transmit-shreds-stats
```

That query groups by `host_id` and `is_xdp`, so the script can distinguish:

- validators explicitly reporting `is_xdp=true`
- validators explicitly reporting `is_xdp=false`
- validators with both states in the selected time window
- staked validators that are not reporting dashboard metrics

### XDP Off Report

`--is-xdp-off` replaces the final XDP host table with the top-staked validators that only reported `is_xdp=false`.

```bash
python3 xdp_stake_coverage.py mainnet --is-xdp-off
```

### Top-Staked XDP Status Report

`--top-staked-xdp-status` prints:

- top 10, 20, 50, and 100 staked validators explicitly reporting `is_xdp=false`
- top 10, 20, 50, and 100 staked validators not reporting dashboard metrics
- top 100 staked validators explicitly reporting `is_xdp=true`

Each row includes:

```text
pubkey,validator_name,stake_percent_of_total,is_xdp_states
```

The non-reporting table omits `is_xdp_states` because those validators have no XDP metric state in the queried data.

Validator names are looked up from validators.app by default. If `--validators-app-token` or `VALIDATORS_APP_TOKEN` is set, the script tries the validators.app API first. If validators.app does not resolve a name, the script falls back to `solana validator-info get --output json`. Use `--no-validator-names` to skip name lookup.

### Packet Fraction Report

`--packet-fractions` queries `sum(total_packets)` from the dashboard measurement, grouped by `host_id` and `is_xdp`, and prints the fraction of packets broadcast by leaders with XDP on versus off.

When used alone, it prints only:

```text
scope,leader_xdp_on_packet_fraction,leader_xdp_off_packet_fraction
```

Scopes are:

- `metric_reporting_validators`
- `all_validators`
- `top_10_staked_validators`
- `top_20_staked_validators`
- `top_50_staked_validators`
- `top_<N>_staked_validators`, where `N` is `--top-staked-count`

`--packet-fractions` can also be combined with the normal report and `--top-staked-xdp-status`.

## CSV Inputs and Outputs

For normal stake coverage, `--csv` must contain a host identity pubkey column. The default column is `host_id`; override it with `--csv-host-column`.

For `--is-xdp-off` or `--top-staked-xdp-status`, the CSV must contain:

```text
host_id,is_xdp
```

When querying Influx, the script writes query rows to the network default CSV unless `--no-write-xdp-csv` is set. Use `--xdp-csv-out` to choose a different output path.

## CLI Reference

```text
usage: xdp_stake_coverage.py [-h] [--csv CSV]
                             [--xdp-source {retransmit-stage,broadcast-transmit-shreds-stats}]
                             [--is-xdp-off] [--packet-fractions]
                             [--top-staked-xdp-status] [--no-validator-names]
                             [--validators-app-token VALIDATORS_APP_TOKEN]
                             [--validator-name-timeout VALIDATOR_NAME_TIMEOUT]
                             [--packet-measurement PACKET_MEASUREMENT]
                             [--xdp-csv-out XDP_CSV_OUT] [--no-write-xdp-csv]
                             [--stake-json STAKE_JSON] [--skip-stake-refresh]
                             [--solana-bin SOLANA_BIN]
                             [--solana-timeout SOLANA_TIMEOUT]
                             [--csv-host-column CSV_HOST_COLUMN]
                             [--env-file ENV_FILE] [--influx-db INFLUX_DB]
                             [--retention-policy RETENTION_POLICY]
                             [--measurement MEASUREMENT] [--hours HOURS]
                             [--interval INTERVAL]
                             [--influx-scheme {http,https}]
                             [--influx-timeout INFLUX_TIMEOUT]
                             [--influx-auth {auto,basic,query-params,body-params}]
                             [--identity-key IDENTITY_KEY]
                             [--stake-key STAKE_KEY]
                             [--total-stake-key TOTAL_STAKE_KEY]
                             [--top-staked-count TOP_STAKED_COUNT]
                             [{mainnet,testnet}]
```

| Option | Default | Description |
| --- | --- | --- |
| `network` | `testnet` | Network file set to use. Choices: `mainnet`, `testnet`. |
| `--csv CSV` | none | Read XDP host IDs from this CSV instead of querying Influx. |
| `--xdp-source` | `retransmit-stage` | Influx query source when `--csv` is not supplied. Automatically switches to `broadcast-transmit-shreds-stats` for `--is-xdp-off` and `--top-staked-xdp-status`. |
| `--is-xdp-off` | false | Replace the final XDP host table with top-staked validators that only reported `is_xdp=false`. |
| `--packet-fractions` | false | Print packet fractions by leader XDP state for metric reporters, all validators, and top-staked validators. |
| `--top-staked-xdp-status` | false | Print top 10, 20, 50, and 100 staked validator XDP status sections, plus the top 100 `is_xdp=true` section. |
| `--no-validator-names` | false | Do not look up validator names for `--top-staked-xdp-status`. |
| `--validators-app-token` | `VALIDATORS_APP_TOKEN` or `VALIDATORS_APP_API_TOKEN` | Optional validators.app API token. Without it, names are read from public validators.app pages. |
| `--validator-name-timeout` | `15` | validators.app HTTP timeout in seconds for validator name lookup. |
| `--packet-measurement` | `broadcast-transmit-shreds-stats` | Influx measurement to query for `--packet-fractions`. |
| `--xdp-csv-out` | network CSV path | Where to write fetched Influx XDP query rows. |
| `--no-write-xdp-csv` | false | Do not write fetched Influx rows to CSV. |
| `--stake-json` | network stake JSON path | Validator stake JSON path. |
| `--skip-stake-refresh` | false | Use the existing stake JSON instead of running `solana validators`. |
| `--solana-bin` | `solana` | Solana CLI binary used for stake refresh and validator-info fallback. |
| `--solana-timeout` | `180` | Solana CLI timeout in seconds. |
| `--csv-host-column` | `host_id` | CSV column containing host identity pubkeys. |
| `--env-file` | `.env` | Env file with Influx settings. |
| `--influx-db` | network database | Influx database override. |
| `--retention-policy` | `autogen` | Influx retention policy. |
| `--measurement` | source-dependent | Influx measurement override. Defaults to `retransmit-stage` or `broadcast-transmit-shreds-stats` based on `--xdp-source`. |
| `--hours` | `12` | Number of trailing hours of Influx data to query. |
| `--interval` | `1m` | Influx `GROUP BY time` interval. |
| `--influx-scheme` | `https` | Influx HTTP scheme. Choices: `http`, `https`. |
| `--influx-timeout` | `300` | Influx HTTP timeout in seconds. |
| `--influx-auth` | `auto` | Influx authentication placement. Choices: `auto`, `basic`, `query-params`, `body-params`. |
| `--identity-key` | `identityPubkey` | Validator JSON key matching the CSV host ID. |
| `--stake-key` | `activatedStake` | Validator JSON stake field to sum. |
| `--total-stake-key` | `totalActiveStake` | Top-level JSON total stake field. Falls back to the sum of validators if absent. |
| `--top-staked-count` | `100` | Number of highest-staked validators to summarize in the general top-staked and packet-fraction reports. |

## Examples

Status report without name lookup:

```bash
python3 xdp_stake_coverage.py mainnet --top-staked-xdp-status --no-validator-names
```

Status report from an existing dashboard CSV:

```bash
python3 xdp_stake_coverage.py mainnet \
  --csv data/mainnet-xdp-data.csv \
  --skip-stake-refresh \
  --top-staked-xdp-status
```

Packet fractions for a shorter time window:

```bash
python3 xdp_stake_coverage.py mainnet --packet-fractions --hours 1 --interval 30s
```

Combined report:

```bash
python3 xdp_stake_coverage.py mainnet \
  --packet-fractions \
  --top-staked-xdp-status \
  --is-xdp-off
```

