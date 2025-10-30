package galexie

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/stellar/go/support/datastore"
)

// findResumeLedger determines the first missing ledger sequence within the requested range [start, end].
// It is used to decide whether an export can resume and from which ledger.
//
// Parameters:
//   - start: The inclusive lower bound of the search range. Must be >= 1.
//   - end:   The inclusive upper bound of the search range. If set to 0, the range is treated as
//     open-ended and the latest ledger in the datastore is used as an effective upper limit.
//
// Behavior:
//   - If start < 1, an error is returned.
//   - If end == 0, the search scans up to the most recent ledger found in the datastore.
//   - If no ledger files exist in the datastore, we assume nothing has been exported yet and resume from `start`.
//   - If the latest known ledger < start, resume from `start`.
//   - If end != 0 and the datastore already contains all ledgers up to `end`, there is nothing to resume — returns 0.
//   - Otherwise, returns the first missing ledger, which is `latest + 1`.
//
// Returns:
//   - absentLedger: The first missing ledger sequence to resume from. Returns 0 if no resume is needed.
//   - err:          A non-nil error if the search failed or the context was canceled.
//
// Truth table for outputs (when err == nil):
//  1. No ledgers found in datastore:     absentLedger = start
//  2. Partial range present:            absentLedger = latest + 1 (where start <= latest < end)
//  3. All ledgers present in range:     absentLedger = 0
func findResumeLedger(ctx context.Context, dataStore datastore.DataStore, schema datastore.DataStoreSchema,
	start, end uint32) (absentLedger uint32, err error) {
	if start < 1 {
		return 0, errors.New("Invalid start value, must be greater than zero")
	}

	if end != 0 && end < start {
		return 0, fmt.Errorf("end %d cannot be less than start %d", end, start)
	}

	var latestLedger uint32
	var findErr error

	if end == 0 {
		latestLedger, findErr = datastore.FindLatestLedgerSequence(ctx, dataStore)
	} else {
		latestLedger, findErr = datastore.FindLatestLedgerUpToSequence(ctx, dataStore, end, schema)
	}

	if findErr != nil {
		if errors.Is(findErr, datastore.ErrNoValidLedgerFiles) {
			return start, nil
		}
		return 0, fmt.Errorf("failed to find the latest ledger sequence: %w", findErr)
	}

	if latestLedger < start {
		return start, nil
	}

	if latestLedger >= end && end != 0 {
		return 0, nil
	}

	return latestLedger + 1, nil
}
