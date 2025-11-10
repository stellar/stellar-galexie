package scan

import (
	"context"
	"fmt"

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

	it := &LedgerKeyIter{
		DS:         ds,
		StartAfter: schema.GetObjectKeyFromSequenceNumber(partition.high + 1),
		StopAfter:  schema.GetObjectKeyFromSequenceNumber(partition.low),
	}

	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}

		keys, err := it.Next(ctx)
		if err != nil {
			return res, err
		}
		if len(keys) == 0 {
			break
		}

		for _, cur := range keys {
			if err := ctx.Err(); err != nil {
				return res, err
			}

			if res.count == 0 {
				// First file: set high watermark and check top boundary gap.
				res.high = cur.high
				if cur.high < partition.high {
					res.gaps = append(res.gaps, Gap{
						Start: cur.high + 1,
						End:   partition.high,
					})
				}
			} else {
				// Internal gap
				if res.low > 0 && cur.high+1 < res.low {
					res.gaps = append(res.gaps, Gap{
						Start: cur.high + 1,
						End:   res.low - 1,
					})
				}
			}

			// Count and advance low watermark.
			if cur.high < cur.low {
				return res, fmt.Errorf("invalid range: %d-%d", cur.low, cur.high)
			}
			res.count += cur.high - cur.low + 1
			res.low = cur.low
		}
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
