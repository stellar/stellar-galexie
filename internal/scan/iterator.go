package scan

import (
	"context"
	"fmt"
	"iter"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/stellar/go/support/datastore"
)

var ledgerFilenameRe = regexp.MustCompile(`^[0-9A-F]{8}--[0-9]+(?:-[0-9]+)?\.xdr\.[A-Za-z0-9._-]+$`)

type LedgerFile struct {
	key  string
	high uint32
	low  uint32
}

type LedgerFileIter struct {
	DS         datastore.DataStore
	StartAfter string // upper bound: iteration starts *after* this key
	StopAfter  string // lower bound: stop once we reach or go below this key
}

// Next returns an iterator (iter.Seq2) that lazily yields ledger objects from the
// underlying datastore in descending order.
//
// Each iteration step queries the datastore for the next batch of file paths
// using the current StartAfter marker. For every valid ledger file, it parses
// the corresponding ledger range and yields a LedgerObject along with any
// error encountered.
//
// The iterator stops naturally when no more files are available, or early if
// the provided context is canceled or an error occurs. In the case of an error,
// the yielded error will be non-nil and iteration will terminate.
func (it *LedgerFileIter) Next(ctx context.Context) iter.Seq2[LedgerFile, error] {
	return func(yield func(LedgerFile, error) bool) {
		startAfter := it.StartAfter

		for {
			if err := ctx.Err(); err != nil {
				yield(LedgerFile{}, err)
				return
			}

			paths, err := it.DS.ListFilePaths(ctx, datastore.ListFileOptions{StartAfter: startAfter, Limit: 2})
			if err != nil {
				yield(LedgerFile{}, err)
				return
			}
			if len(paths) == 0 {
				return
			}

			startAfter = paths[len(paths)-1]

			for _, p := range paths {
				if it.StopAfter != "" && p > it.StopAfter {
					return
				}

				base := filepath.Base(p)
				if !ledgerFilenameRe.MatchString(base) {
					continue
				}

				low, high, err := parseRangeFromFilename(base)
				if err != nil {
					yield(LedgerFile{}, fmt.Errorf("parse ledger range for %s: %w", p, err))
					return
				}

				if !yield(LedgerFile{key: p, low: low, high: high}, nil) {
					return
				}
			}
		}
	}
}

var keyRangeRE = regexp.MustCompile(`--(\d+)(?:-(\d+))?\.xdr\.`)

func parseRangeFromFilename(base string) (uint32, uint32, error) {
	m := keyRangeRE.FindStringSubmatch(base)
	if m == nil || len(m) < 2 {
		return 0, 0, fmt.Errorf("invalid file name %q", base)
	}

	parseUint32 := func(s, label string) (uint32, error) {
		u, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("error parsing %s %q: %w", label, s, err)
		}
		return uint32(u), nil
	}

	var low uint32
	var high uint32
	var err error

	low, err = parseUint32(m[1], "low")
	if err != nil {
		return 0, 0, err
	}

	// If low is present and non-empty, parse it; otherwise low == high.
	if len(m) >= 3 && m[2] != "" {
		high, err = parseUint32(m[2], "high")
		if err != nil {
			return 0, 0, err
		}
	} else {
		high = low
	}

	if low > high {
		return 0, 0, fmt.Errorf("invalid ledger range in %q: low (%d) > high (%d)", base, low, high)
	}

	return low, high, nil
}
