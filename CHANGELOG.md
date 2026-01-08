# Changelog

All notable changes to this project will be documented in this
file. This project adheres to [Semantic Versioning](http://semver.org/).

## [v25.1.0]

### New Features
- Added `filesystem` datastore support ([#53](https://github.com/stellar/stellar-galexie/pull/53))
   - Galexie can now be configured to use local filesystem storage in addition to GCS and S3.
   - Intended for testing and development, not production use.
- Added `version` command ([#54](https://github.com/stellar/stellar-galexie/pull/54))
   - New `stellar-galexie version` command displays the application version.

## [v25.0.0]

## New Features
 - Added `futurenet` support to Galexie network configuration parameter ([#42](https://github.com/stellar/stellar-galexie/pull/42)).
 - Added `detect-gaps` Command ([#43](https://github.com/stellar/stellar-galexie/pull/43))
    - New galexie `detect-gaps` command for scanning datastore to find any missing ledger sequences (gaps) within a specified range.
    - The command scans a ledger range defined by `--start` and `--end`.
    - Reports can be printed to `stdout` or saved as a JSON file using the `--output-file` flag.

    Usage:
    ```
         galexie detect-gaps --start <start_ledger> --end <end_ledger> --output-file gaps.json
    ```

### Bug Fixes

- Updated `go-stellar-sdk` to pick up the fix for captive core online mode not replaying sequence 2 ([#5866](https://github.com/stellar/go-stellar-sdk/issues/5866), [#36](https://github.com/stellar/stellar-galexie/issues/36))

## [v24.1.0]

### New Features

- Introduced a new `replace` command ([#5826](https://github.com/stellar/go-stellar-sdk/pull/5826)).
  This command allows users to fully re-export all ledgers within a specified range, explicitly overwriting any existing data files in the destination data lake.
   `stellar-galexie replace --start <start> --end <end>`

### Bug Fixes

- Correctly handle destination bucket paths that include a trailing slash ([#5831](https://github.com/stellar/go-stellar-sdk/pull/5831))

## [v24.0.0]

### New Features
 - Added new sub-command `load-test` to perform load testing on Galexie export - ([#5820](https://github.com/stellar/go-stellar-sdk/pull/5820)). It uses the (ingest/loadtest)[https://github.com/stellar/go-stellar-sdk/tree/master/ingest/loadtest] sdk tool which generates synthetic ledgers at runtime from a pre-built synthetic ledgers data file. You must create the synthetic ledgers data file first with (ingest/loadtest generator tool)[../horizon/internal/integration/generate_ledgers_test.go]. 
   ```
   ./galexie load-test --help
   ```

## [v23.0.0]

### New Features
 - Galexie can be configured to use S3 (or services which have an S3 compatible API) instead of GCS for storage ([#5748](https://github.com/stellar/go-stellar-sdk/pull/5748))

### Breaking Changes
⚠ This is a breaking change that requires a one-time update to your bucket. For detailed instructions, please see [UPGRADE.md](./UPGRADE.md).

 - Galexie now complies with [SEP-0054](https://github.com/stellar/stellar-protocol/blob/master/ecosystem/sep-0054.md) ([#5773](https://github.com/stellar/go-stellar-sdk/pull/5773))
    - Ledger file extension changed from `.zstd` to `.zst` (standard Zstandard compression extension).
    - Galexie will create a new .config.json manifest file in the data lake on its first run if one doesn't already exist.

## [v1.0.0] 

- 🎉 First release!
