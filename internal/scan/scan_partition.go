package scan

import (
	"context"
	"fmt"
	"math"

	"github.com/stellar/go/support/datastore"
)

// scanPartition walks a partition in DESC order and records gaps.
func scanPartition(
	ctx context.Context,
	partition Partition,
	ds datastore.DataStore,
	schema datastore.DataStoreSchema,
) (Result, error) {
	res := Result{gaps: make([]Gap, 0)}

	// StartAfter normally uses (high+1); if high==MaxUint32, leave empty to mean "no upper bound".
	startAfter := ""
	if partition.high != math.MaxUint32 {
		startAfter = schema.GetObjectKeyFromSequenceNumber(partition.high + 1)
	}
	stopAfter := schema.GetObjectKeyFromSequenceNumber(partition.low)

	for cur, err := range datastore.LedgerFileIter(ctx, ds, startAfter, stopAfter) {
		if err != nil {
			return res, err
		}

		if cur.High < cur.Low {
			return res, fmt.Errorf("invalid range: %d-%d", cur.Low, cur.High)
		}

		if res.count == 0 {
			// First file: set high watermark and check top boundary gap.
			res.high = cur.High
			if cur.High < partition.high {
				res.gaps = append(res.gaps, Gap{
					Start: cur.High + 1,
					End:   partition.high,
				})
			}
		} else if res.low > 0 && cur.High != math.MaxUint32 && cur.High+1 < res.low {
			// Internal gap: guard (+1)
			res.gaps = append(res.gaps, Gap{
				Start: cur.High + 1,
				End:   res.low - 1,
			})
		}

		// Avoid uint32 overflow
		delta64 := uint64(cur.High) - uint64(cur.Low) + 1
		if delta64 > uint64(math.MaxUint32) {
			return res, fmt.Errorf("delta overflow: range %d-%d spans %d ledgers (> uint32 max)", cur.Low, cur.High, delta64)
		}

		sum64 := uint64(res.count) + delta64
		if sum64 > uint64(math.MaxUint32) {
			return res, fmt.Errorf("count overflow: %d + %d exceeds uint32 max", res.count, delta64)
		}
		// Count and advance low watermark.
		res.count = uint32(sum64)
		res.low = cur.Low
	}

	// Final boundary reconciliation.
	if res.count == 0 {
		// Entire partition is missing.
		res.gaps = append(res.gaps, Gap{Start: partition.low, End: partition.high})
		return res, nil
	}
	if res.low > partition.low {
		// Bottom boundary gap.
		res.gaps = append(res.gaps, Gap{Start: partition.low, End: res.low - 1})
	}
	return res, nil
}
