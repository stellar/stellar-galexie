package galexie

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/support/datastore"
)

func TestResumability(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name              string
		startLedger       uint32
		endLedger         uint32
		dataStoreSchema   datastore.DataStoreSchema
		absentLedger      uint32
		latestLedger      uint32
		errorSnippet      string
		registerMockCalls func(*datastore.MockDataStore)
	}{
		{
			name:         "End ledger same as start, data store has it",
			startLedger:  4,
			endLedger:    4,
			absentLedger: 0,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			registerMockCalls: func(mockDataStore *datastore.MockDataStore) {
				mockDataStore.On("ListFilePaths", ctx, datastore.ListFileOptions{
					StartAfter: "FFFFFFF5--10-19.xdr.zst"}).Return([]string{"FFFFFFFF--0-9.xdr.zst"}, nil).Once()
				mockDataStore.On("GetFileMetadata", ctx, "FFFFFFFF--0-9.xdr.zst").
					Return(map[string]string{"end-ledger": "9"}, nil).Once()
			},
		},
		{
			name:         "End ledger same as start, data store does not have it",
			startLedger:  14,
			endLedger:    14,
			absentLedger: 14,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			registerMockCalls: func(mockDataStore *datastore.MockDataStore) {
				mockDataStore.On("ListFilePaths", ctx, datastore.ListFileOptions{
					StartAfter: "FFFFFFEB--20-29.xdr.zst"}).Return([]string{""}, nil).Once()
			},
		},
		{
			name:         "start and end ledger are in same file, data store does not have it",
			startLedger:  64,
			endLedger:    68,
			absentLedger: 64,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(100),
				LedgersPerFile:    uint32(64),
			},
			registerMockCalls: func(mockDataStore *datastore.MockDataStore) {
				mockDataStore.On("ListFilePaths", ctx, datastore.ListFileOptions{
					StartAfter: "FFFFFFFF--0-6399/FFFFFF7F--128-191.xdr.zst"}).Return([]string{""}, nil).Once()
			},
		},
		{
			name:         "start and end ledger are in same file, data store has it",
			startLedger:  128,
			endLedger:    130,
			absentLedger: 0,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(100),
				LedgersPerFile:    uint32(64),
			},
			registerMockCalls: func(mockDataStore *datastore.MockDataStore) {
				mockDataStore.On("ListFilePaths", ctx, datastore.ListFileOptions{
					StartAfter: "FFFFFFFF--0-6399/FFFFFF3F--192-255.xdr.zst"}).
					Return([]string{"FFFFFFFF--0-6399/FFFFFF7F--128-191.xdr.zst"}, nil).Once()
				mockDataStore.On("GetFileMetadata", ctx, "FFFFFFFF--0-6399/FFFFFF7F--128-191.xdr.zst").
					Return(map[string]string{"end-ledger": "191"}, nil).Once()
			},
		},
		{
			name:         "ledger range overlaps with a range which is already exported",
			startLedger:  2,
			endLedger:    127,
			absentLedger: 0,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(100),
				LedgersPerFile:    uint32(64),
			},
			registerMockCalls: func(mockDataStore *datastore.MockDataStore) {
				mockDataStore.On("ListFilePaths", ctx, datastore.ListFileOptions{
					StartAfter: "FFFFFFFF--0-6399/FFFFFF7F--128-191.xdr.zst"}).
					Return([]string{"FFFFFFFF--0-6399/FFFFFF7F--128-191.xdr.zst"}, nil).Once()
				mockDataStore.On("GetFileMetadata", ctx, "FFFFFFFF--0-6399/FFFFFF7F--128-191.xdr.zst").
					Return(map[string]string{"end-ledger": "191"}, nil).Once()
			},
		},
		{
			name:         "error encountered while finding the latest ledger in datastore",
			startLedger:  24,
			endLedger:    24,
			absentLedger: 0,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			errorSnippet: "datastore error happened",
			registerMockCalls: func(mockDataStore *datastore.MockDataStore) {
				mockDataStore.On("ListFilePaths", ctx, datastore.ListFileOptions{
					StartAfter: "FFFFFFE1--30-39.xdr.zst"}).
					Return([]string{""}, errors.New("datastore error happened")).Once()
			},
		},
		{
			name:         "Data store is beyond boundary aligned start ledger",
			startLedger:  20,
			endLedger:    50,
			absentLedger: 40,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			registerMockCalls: func(mockDataStore *datastore.MockDataStore) {
				mockDataStore.On("ListFilePaths", ctx, datastore.ListFileOptions{
					StartAfter: "FFFFFFC3--60-69.xdr.zst"}).Return([]string{"FFFFFFE1--30-39.xdr.zst"}, nil).Once()
				mockDataStore.On("GetFileMetadata", ctx, "FFFFFFE1--30-39.xdr.zst").
					Return(map[string]string{"end-ledger": "39"}, nil).Once()
			},
		},
		{
			name:         "Data store is beyond non boundary aligned start ledger",
			startLedger:  55,
			endLedger:    85,
			absentLedger: 80,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			registerMockCalls: func(mockDataStore *datastore.MockDataStore) {
				mockDataStore.On("ListFilePaths", ctx, datastore.ListFileOptions{
					StartAfter: "FFFFFFA5--90-99.xdr.zst"}).Return([]string{"FFFFFFB9--70-79.xdr.zst"}, nil).Once()
				mockDataStore.On("GetFileMetadata", ctx, "FFFFFFB9--70-79.xdr.zst").
					Return(map[string]string{"end-ledger": "79"}, nil).Once()
			},
		},
		{
			name:         "Data store is beyond start and end ledger",
			startLedger:  255,
			endLedger:    275,
			absentLedger: 0,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			registerMockCalls: func(mockDataStore *datastore.MockDataStore) {
				mockDataStore.On("ListFilePaths", ctx, datastore.ListFileOptions{
					StartAfter: "FFFFFEE7--280-289.xdr.zst"}).
					Return([]string{"FFFFFEF1--270-279.xdr.zst", "FFFFFEFB--260-269.xdr.zst"}, nil).Once()
				mockDataStore.On("GetFileMetadata", ctx, "FFFFFEF1--270-279.xdr.zst").
					Return(map[string]string{"end-ledger": "279"}, nil).Once()
			},
		},
		{
			name:         "Data store is not beyond start ledger",
			startLedger:  95,
			endLedger:    125,
			absentLedger: 95,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			registerMockCalls: func(mockDataStore *datastore.MockDataStore) {
				mockDataStore.On("ListFilePaths", ctx, datastore.ListFileOptions{
					StartAfter: "FFFFFF7D--130-139.xdr.zst"}).Return([]string{""}, nil).Once()
			},
		},
		{
			name:         "No start ledger provided",
			startLedger:  0,
			endLedger:    10,
			absentLedger: 0,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			errorSnippet:      "Invalid start value",
			registerMockCalls: func(store *datastore.MockDataStore) {},
		},
		{
			name:         "No end ledger provided, data store not beyond start",
			startLedger:  1145,
			endLedger:    0,
			absentLedger: 1145,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			latestLedger: uint32(2000),
			registerMockCalls: func(mockDataStore *datastore.MockDataStore) {
				mockDataStore.On("ListFilePaths", ctx, datastore.ListFileOptions{}).
					Return([]string{"FFFFFEF1--270-279.xdr.zst", "FFFFFEFB--260-269.xdr.zst"}, nil).Once()
				mockDataStore.On("GetFileMetadata", ctx, "FFFFFEF1--270-279.xdr.zst").
					Return(map[string]string{"end-ledger": "279"}, nil).Once()
			},
		},
		{
			name:         "No end ledger provided, data store is beyond start",
			startLedger:  2145,
			endLedger:    0,
			absentLedger: 2250,
			dataStoreSchema: datastore.DataStoreSchema{
				FilesPerPartition: uint32(1),
				LedgersPerFile:    uint32(10),
			},
			latestLedger: uint32(3000),
			registerMockCalls: func(mockDataStore *datastore.MockDataStore) {
				mockDataStore.On("ListFilePaths", ctx, datastore.ListFileOptions{}).
					Return([]string{"FFFFF73F--2240-2249.xdr.zst", "FFFFF749--2230-2239.xdr.zst"}, nil).Once()
				mockDataStore.On("GetFileMetadata", ctx, "FFFFF73F--2240-2249.xdr.zst").
					Return(map[string]string{"end-ledger": "2249"}, nil).Once()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDataStore := &datastore.MockDataStore{}
			tt.registerMockCalls(mockDataStore)

			absentLedger, err := findResumeLedger(ctx, mockDataStore, tt.dataStoreSchema, tt.startLedger, tt.endLedger)
			if tt.errorSnippet != "" {
				require.ErrorContains(t, err, tt.errorSnippet)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.absentLedger, absentLedger)

			mockDataStore.AssertExpectations(t)
		})
	}

}
