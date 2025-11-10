package scan

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/stellar/go/support/datastore"
)

var ledgerFilenameRe = regexp.MustCompile(`^[0-9A-F]{8}--[0-9]+(?:-[0-9]+)?\.xdr\.[A-Za-z0-9._-]+$`)

type LedgerObject struct {
	key  string
	high uint32
	low  uint32
}

type LedgerKeyIter struct {
	DS         datastore.DataStore
	StartAfter string // upper bound: iteration starts *after* this key
	StopAfter  string // lower bound: stop once we reach or go below this key
	reachedEnd bool
}

func (it *LedgerKeyIter) Next(ctx context.Context) ([]LedgerObject, error) {
	if it.reachedEnd {
		return nil, nil
	}

	opts := datastore.ListFileOptions{StartAfter: it.StartAfter}
	paths, err := it.DS.ListFilePaths(ctx, opts)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, nil
	}

	it.StartAfter = paths[len(paths)-1]

	objs := make([]LedgerObject, 0, len(paths))
	for _, p := range paths {
		base := filepath.Base(p)
		if !ledgerFilenameRe.MatchString(base) {
			continue
		}

		if it.StopAfter != "" && p > it.StopAfter {
			it.reachedEnd = true
			break
		}

		low, high, err := parseRangeFromFilename(base)
		if err != nil {
			return nil, fmt.Errorf("parse ledger range for %s: %w", p, err)
		}

		objs = append(objs, LedgerObject{key: p, low: low, high: high})
	}
	return objs, nil
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
