package scan

import (
	"context"
	"sort"
	"testing"

	"github.com/stellar/go/support/datastore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestParseRangeFromFilename_SingleAndRange(t *testing.T) {
	low, high, err := parseRangeFromFilename("0000000A--10.xdr.zst")
	require.NoError(t, err)
	require.Equal(t, uint32(10), low)
	require.Equal(t, uint32(10), high)

	low, high, err = parseRangeFromFilename("00000014--11-20.xdr.zst")
	require.NoError(t, err)
	require.Equal(t, uint32(11), low)
	require.Equal(t, uint32(20), high)
}

func TestLedgerKeyIter_Pagination(t *testing.T) {
	ctx := context.Background()
	ds := new(datastore.MockDataStore)

	// Page 1
	ds.On("ListFilePaths", mock.Anything, mock.MatchedBy(func(o datastore.ListFileOptions) bool {
		return o.StartAfter == ""
	})).Return([]string{
		"0000000A--10.xdr.zst",
		"00000014--11-20.xdr.zst",
	}, nil).Once()

	// Page 2 (StartAfter = last of page 1)
	ds.On("ListFilePaths", mock.Anything, mock.MatchedBy(func(o datastore.ListFileOptions) bool {
		return o.StartAfter == "00000014--11-20.xdr.zst"
	})).Return([]string{
		"0000001E--21-30.xdr.zst",
	}, nil).Once()

	// Empty page terminates
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string{}, nil).Maybe()

	var all []LedgerFile
	for cur, err := range LedgerFileIter(ctx, ds, "", "") {
		require.NoError(t, err)
		all = append(all, cur)
	}

	require.Len(t, all, 3)
	keys := []string{all[0].key, all[1].key, all[2].key}
	exp := []string{"0000000A--10.xdr.zst", "00000014--11-20.xdr.zst", "0000001E--21-30.xdr.zst"}
	require.Equal(t, exp, keys)

	ds.AssertExpectations(t)
}

func TestParseRangeFromFilename(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		low     uint32
		high    uint32
		wantErr bool
	}{
		{"single ledger", "ABCDEF01--7.xdr.foo", 7, 7, false},
		{"range ok", "ABCDEF01--42-100.xdr.bar", 42, 100, false},
		{"low==high ok", "00000000--5-5.xdr.baz", 5, 5, false},
		{"invalid no marker", "weirdname.xdr", 0, 0, true},
		{"low>high", "ABCDEF01--10-9.xdr.foo", 0, 0, true},
		{"bad low", "ABCDEF01--notnum-9.xdr.foo", 0, 0, true},
		{"bad high", "ABCDEF01--10-notnum.xdr.foo", 0, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lo, hi, err := parseRangeFromFilename(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if lo != tc.low || hi != tc.high {
				t.Fatalf("got %d-%d, want %d-%d", lo, hi, tc.low, tc.high)
			}
		})
	}
}
func TestLedgerKeyIter_StopAfter(t *testing.T) {
	ctx := context.Background()
	ds := new(datastore.MockDataStore)

	// Single page returned; iterator will stop early when p > StopAfter
	page := []string{
		"0000000A--10.xdr.zst",
		"00000014--11-20.xdr.zst",
		"0000001E--21-30.xdr.zst",
	}
	sort.Strings(page)
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return(page, nil).Once()
	ds.On("ListFilePaths", mock.Anything, mock.Anything).
		Return([]string{}, nil).Maybe()

	stopAfter := "00000014--11-20.xdr.zst" // stop once > this
	var all []LedgerFile
	for cur, err := range LedgerFileIter(ctx, ds, "", stopAfter) {
		require.NoError(t, err)
		all = append(all, cur)
	}
	// Should include entries <= StopAfter
	require.Len(t, all, 2)

	ds.AssertExpectations(t)
}

func TestLedgerFileIter_InvalidRange_YieldsError(t *testing.T) {
	ctx := context.Background()
	ds := new(datastore.MockDataStore)
	iter := LedgerFileIter(ctx, ds, "zzz", "aaa") // startAfter > stopAfter
	for lf, err := range iter {
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid range")
		require.Zero(t, lf)
	}
	ds.AssertExpectations(t)
}
