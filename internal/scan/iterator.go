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

// LedgerFileIter returns an iterator (iter.Seq2) over ledger files whose file
// paths fall in the lexicographic range [startAfter, stopAfter]. Paths are
// listed in ascending lexicographic order.
//
// Both bounds are optional:
//   - If startAfter == "", iteration begins at the first file.
//   - If stopAfter == "", the upper bound is unbounded.
//
// If both bounds are provided and stopAfter <= startAfter, the range is empty
// and the iterator yields a single error.
//
// The iterator stops when:
//   - all matching file paths have been consumed,
//   - the context is canceled, or
//   - an error occurs.
func LedgerFileIter(ctx context.Context, ds datastore.DataStore, startAfter,
	stopAfter string) iter.Seq2[LedgerFile, error] {
	return func(yield func(LedgerFile, error) bool) {
		if startAfter != "" && stopAfter != "" && startAfter >= stopAfter {
			yield(LedgerFile{}, fmt.Errorf("invalid range: startAfter (%q) >= stopAfter (%q)",
				startAfter, stopAfter))
			return
		}

		for {
			paths, err := ds.ListFilePaths(ctx, datastore.ListFileOptions{StartAfter: startAfter})
			if err != nil {
				yield(LedgerFile{}, err)
				return
			}
			if len(paths) == 0 {
				return
			}

			for _, p := range paths {
				if stopAfter != "" && p > stopAfter {
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
			startAfter = paths[len(paths)-1]
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
