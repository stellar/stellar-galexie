package galexie

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestNewLedgerMetaArchiveFromXDR(t *testing.T) {
	data := xdr.LedgerCloseMetaBatch{
		StartSequence: 1234,
		EndSequence:   1234,
		LedgerCloseMetas: []xdr.LedgerCloseMeta{
			createLedgerCloseMeta(1234),
		},
	}

	archive, err := NewLedgerMetaArchiveFromXDR("testnet", "v1.2.3", "key", data)

	require.NoError(t, err)
	require.NotNil(t, archive)

	// Check if the metadata fields are correctly populated
	expectedMetaData := datastore.MetaData{
		StartLedger:          1234,
		EndLedger:            1234,
		StartLedgerCloseTime: 1234 * 100,
		EndLedgerCloseTime:   1234 * 100,
		NetworkPassPhrase:    "testnet",
		CompressionType:      "zst",
		ProtocolVersion:      21,
		CoreVersion:          "v1.2.3",
		Version:              "develop",
	}

	require.Equal(t, expectedMetaData, archive.metaData)

	data = xdr.LedgerCloseMetaBatch{
		StartSequence: 1234,
		EndSequence:   1237,
		LedgerCloseMetas: []xdr.LedgerCloseMeta{
			createLedgerCloseMeta(1234),
			createLedgerCloseMeta(1235),
			createLedgerCloseMeta(1236),
			createLedgerCloseMeta(1237),
		},
	}

	archive, err = NewLedgerMetaArchiveFromXDR("testnet", "v1.2.3", "key", data)

	require.NoError(t, err)
	require.NotNil(t, archive)

	// Check if the metadata fields are correctly populated
	expectedMetaData = datastore.MetaData{
		StartLedger:          1234,
		EndLedger:            1237,
		StartLedgerCloseTime: 1234 * 100,
		EndLedgerCloseTime:   1237 * 100,
		NetworkPassPhrase:    "testnet",
		CompressionType:      "zst",
		ProtocolVersion:      21,
		CoreVersion:          "v1.2.3",
		Version:              "develop",
	}

	require.Equal(t, expectedMetaData, archive.metaData)
}

// Protocol 30 ledgers carry one of the CAP-0088 millisecond StellarValue arms.
// Their whole-second closeTime is still set (closeTime == closeTimeMs / 1000),
// so the object metadata keeps reporting seconds, and the uploader's encoding
// must carry the arms through unchanged.
func TestNewLedgerMetaArchiveFromXDRMillisecondCloseTime(t *testing.T) {
	sig := xdr.LedgerCloseValueSignature{
		NodeId:    xdr.NodeId{Type: xdr.PublicKeyTypePublicKeyTypeEd25519, Ed25519: &xdr.Uint256{1}},
		Signature: xdr.Signature{2},
	}
	signedMs := createLedgerCloseMeta(1234)
	signedMs.V0.LedgerHeader.Header.LedgerVersion = 30
	signedMs.V0.LedgerHeader.Header.ScpValue.Ext = xdr.StellarValueExt{
		V: xdr.StellarValueTypeStellarValueSignedMs,
		SignedMsValue: &xdr.StellarValueSignedMsValue{
			CloseTimeMs:      xdr.TimePointMs(1234*100*1000 + 250),
			LcValueSignature: sig,
		},
	}
	emptyTxSetMs := createLedgerCloseMeta(1235)
	emptyTxSetMs.V0.LedgerHeader.Header.LedgerVersion = 30
	emptyTxSetMs.V0.LedgerHeader.Header.ScpValue.Ext = xdr.StellarValueExt{
		V: xdr.StellarValueTypeStellarValueEmptyTxSetMs,
		ProposedMsValue: &xdr.StellarValueProposedMsValue{
			CloseTimeMs:           xdr.TimePointMs(1235*100*1000 + 750),
			PreviousLedgerVersion: 30,
			LcValueSignature:      sig,
		},
	}
	data := xdr.LedgerCloseMetaBatch{
		StartSequence:    1234,
		EndSequence:      1235,
		LedgerCloseMetas: []xdr.LedgerCloseMeta{signedMs, emptyTxSetMs},
	}

	archive, err := NewLedgerMetaArchiveFromXDR("testnet", "v1.2.3", "key", data)
	require.NoError(t, err)
	require.Equal(t, datastore.MetaData{
		StartLedger:          1234,
		EndLedger:            1235,
		StartLedgerCloseTime: 1234 * 100,
		EndLedgerCloseTime:   1235 * 100,
		NetworkPassPhrase:    "testnet",
		CompressionType:      "zst",
		ProtocolVersion:      30,
		CoreVersion:          "v1.2.3",
		Version:              "develop",
	}, archive.metaData)

	var buf bytes.Buffer
	_, err = compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, &archive.Data).WriteTo(&buf)
	require.NoError(t, err)
	var decoded xdr.LedgerCloseMetaBatch
	_, err = compressxdr.NewXDRDecoder(compressxdr.DefaultCompressor, &decoded).ReadFrom(&buf)
	require.NoError(t, err)
	require.Equal(t, data, decoded)
}
