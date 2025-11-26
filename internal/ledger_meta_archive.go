package galexie

import (
	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// LedgerMetaArchive represents a file with metadata and binary data.
type LedgerMetaArchive struct {
	ObjectKey string
	Data      xdr.LedgerCloseMetaBatch
	metaData  datastore.MetaData
}

// NewLedgerMetaArchiveFromXDR creates a new LedgerMetaArchive instance.
func NewLedgerMetaArchiveFromXDR(networkPassPhrase string, coreVersion string, key string, data xdr.LedgerCloseMetaBatch) (*LedgerMetaArchive, error) {
	startLedger, err := data.GetLedger(uint32(data.StartSequence))
	if err != nil {
		return &LedgerMetaArchive{}, err

	}
	endLedger, err := data.GetLedger(uint32(data.EndSequence))
	if err != nil {
		return &LedgerMetaArchive{}, err
	}

	return &LedgerMetaArchive{
		ObjectKey: key,
		Data:      data,
		metaData: datastore.MetaData{
			StartLedger:          startLedger.LedgerSequence(),
			EndLedger:            endLedger.LedgerSequence(),
			StartLedgerCloseTime: startLedger.LedgerCloseTime(),
			EndLedgerCloseTime:   endLedger.LedgerCloseTime(),
			NetworkPassPhrase:    networkPassPhrase,
			CompressionType:      compressxdr.DefaultCompressor.Name(),
			ProtocolVersion:      endLedger.ProtocolVersion(),
			CoreVersion:          coreVersion,
			Version:              version,
		},
	}, nil
}
