# Changelog

All notable changes to this project will be documented in this
file. This project adheres to [Semantic Versioning](http://semver.org/).

## Pending

### New Features
 - Added new sub-command `load-test` to perform load testing on Galexie export - ([#5820](https://github.com/stellar/go/pull/5820)). It uses the (ingest/loadtest)[https://github.com/stellar/go/tree/master/ingest/loadtest] sdk tool which generates synthetic ledgers at runtime from a pre-built synthetic ledgers data file. You must create the synthetic ledgers data file first with (ingest/loadtest generator tool)[../horizon/internal/integration/generate_ledgers_test.go]. 
   ```
   ./galexie load-test --help
   ```

## [v23.0.0]

### New Features
 - Galexie can be configured to use S3 (or services which have an S3 compatible API) instead of GCS for storage ([#5748](https://github.com/stellar/go/pull/5748))

### Breaking Changes
⚠ This is a breaking change that requires a one-time update to your bucket. For detailed instructions, please see [UPGRADE.md](./UPGRADE.md).

 - Galexie now complies with [SEP-0054](https://github.com/stellar/stellar-protocol/blob/master/ecosystem/sep-0054.md) ([#5773](https://github.com/stellar/go/pull/5773))
    - Ledger file extension changed from `.zstd` to `.zst` (standard Zstandard compression extension).
    - Galexie will create a new .config.json manifest file in the data lake on its first run if one doesn't already exist.

## [v1.0.0] 

- 🎉 First release!
