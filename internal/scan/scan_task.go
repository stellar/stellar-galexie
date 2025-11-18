package scan

import (
	"context"
	"fmt"
	"math"

	"github.com/stellar/go/support/datastore"
)

// scanTask walks a ledger range in descending order and records gaps.
func scanTask(
	ctx context.Context,
	t task,
	ds datastore.DataStore,
	schema datastore.DataStoreSchema,
) (result, error) {
	res := result{gaps: make([]gap, 0)}

	// StartAfter normally uses (high+1); if high == MaxUint32, leave it empty
	// to mean "no upper bound".
	startAfter := ""
	if t.high != math.MaxUint32 {
		startAfter = schema.GetObjectKeyFromSequenceNumber(t.high + 1)
	}
	stopAfter := schema.GetObjectKeyFromSequenceNumber(t.low)

	for cur, err := range datastore.LedgerFileIter(ctx, ds, startAfter, stopAfter) {
		if err != nil {
			return res, err
		}

		if cur.High < cur.Low {
			return res, fmt.Errorf("invalid ledger range: %d-%d", cur.Low, cur.High)
		}

		if res.count == 0 {
			// First file: set the high watermark and check the top boundary gap.
			res.high = cur.High
			if cur.High < t.high {
				res.gaps = append(res.gaps, gap{
					Start: cur.High + 1,
					End:   t.high,
				})
			}
		} else if res.low > 0 && cur.High != math.MaxUint32 && cur.High+1 < res.low {
			// Internal gap (cur.High+1 .. res.low-1).
			res.gaps = append(res.gaps, gap{
				Start: cur.High + 1,
				End:   res.low - 1,
			})
		}

		// Avoid uint32 overflow
		delta64 := uint64(cur.High) - uint64(cur.Low) + 1
		if delta64 > uint64(math.MaxUint32) {
			return res, fmt.Errorf(
				"delta overflow: ledger range %d-%d spans %d ledgers (> uint32 max)",
				cur.Low, cur.High, delta64,
			)
		}

		sum64 := uint64(res.count) + delta64
		if sum64 > uint64(math.MaxUint32) {
			return res, fmt.Errorf(
				"count overflow: %d + %d exceeds uint32 max",
				res.count, delta64,
			)
		}

		// Update count and advance the low watermark.
		res.count = uint32(sum64)
		res.low = cur.Low
	}

	// Final boundary reconciliation.
	if res.count == 0 {
		// Entire task range is missing.
		res.gaps = append(res.gaps, gap{Start: t.low, End: t.high})
		return res, nil
	}

	if res.low > t.low {
		// Bottom boundary gap.
		res.gaps = append(res.gaps, gap{Start: t.low, End: res.low - 1})
	}

	return res, nil
}
