package galexie

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/go/support/compressxdr"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

var testShutdownDelayTime = 300 * time.Millisecond

func TestUploaderSuite(t *testing.T) {
	suite.Run(t, new(UploaderSuite))
}

// UploaderSuite is a test suite for the Uploader.
type UploaderSuite struct {
	suite.Suite
	ctx           context.Context
	mockDataStore datastore.MockDataStore
}

func (s *UploaderSuite) SetupTest() {
	s.ctx = context.Background()
	s.mockDataStore = datastore.MockDataStore{}
}

func (s *UploaderSuite) TearDownTest() {
	s.mockDataStore.AssertExpectations(s.T())
}

func (s *UploaderSuite) TestUploadWithMetadata() {
	key, start, end := "test-1-100", uint32(1), uint32(100)
	archive := NewLedgerMetaArchive(key, start, end)
	for i := start; i <= end; i++ {
		_ = archive.Data.AddLedger(createLedgerCloseMeta(i))
	}
	metadata := datastore.MetaData{
		StartLedger:          start,
		EndLedger:            end,
		StartLedgerCloseTime: 123456789,
		EndLedgerCloseTime:   987654321,
		ProtocolVersion:      3,
		CoreVersion:          "v1.2.3",
		NetworkPassPhrase:    "testnet",
		CompressionType:      "gzip",
		Version:              "1.0.0",
	}
	archive.metaData = metadata
	var capturedBuf bytes.Buffer
	s.mockDataStore.On("PutFileIfNotExists", mock.Anything, key, mock.Anything, metadata.ToMap()).
		Run(func(args mock.Arguments) {
			_ = args.Get(1).(string)
			_, err := args.Get(2).(io.WriterTo).WriteTo(&capturedBuf)
			s.Require().NoError(err)
		}).Return(true, nil).Once()

	registry := prometheus.NewRegistry()
	queue := NewUploadQueue(1, registry)
	dataUploader := NewUploader(&s.mockDataStore, queue, registry, false)
	s.Require().NoError(dataUploader.Upload(context.Background(), archive))

}

func (s *UploaderSuite) TestUploadPaths() {
	key, start, end := "test-1-100", uint32(1), uint32(100)
	archive := NewLedgerMetaArchive(key, start, end)
	for i := start; i <= end; i++ {
		_ = archive.Data.AddLedger(createLedgerCloseMeta(i))
	}

	type tc struct {
		name          string
		overwrite     bool
		alreadyExists bool
	}
	cases := []tc{
		{name: "scan-and-fill: created (not exists)", overwrite: false, alreadyExists: false},
		{name: "scan-and-fill: skipped (already exists)", overwrite: false, alreadyExists: true},
		{name: "replace: always write", overwrite: true, alreadyExists: false},
	}

	for _, c := range cases {
		s.Run(c.name, func() {
			var capturedBuf bytes.Buffer
			var capturedKey string

			if c.overwrite {
				s.mockDataStore.
					On("PutFile", mock.Anything, key, mock.Anything, datastore.MetaData{}.ToMap()).
					Run(func(args mock.Arguments) {
						capturedKey = args.Get(1).(string)
						_, err := args.Get(2).(io.WriterTo).WriteTo(&capturedBuf)
						s.Require().NoError(err)
					}).Return(nil).Once()
			} else {
				s.mockDataStore.
					On("PutFileIfNotExists", mock.Anything, key, mock.Anything, datastore.MetaData{}.ToMap()).
					Run(func(args mock.Arguments) {
						capturedKey = args.Get(1).(string)
						_, err := args.Get(2).(io.WriterTo).WriteTo(&capturedBuf)
						s.Require().NoError(err)
					}).Return(!c.alreadyExists, nil).Once()
			}

			registry := prometheus.NewRegistry()
			queue := NewUploadQueue(1, registry)
			uploader := NewUploader(&s.mockDataStore, queue, registry, c.overwrite)
			s.Require().NoError(uploader.Upload(context.Background(), archive))

			expectedCompressedLength := capturedBuf.Len()
			var decodedArchive LedgerMetaArchive
			xdrDecoder := compressxdr.NewXDRDecoder(compressxdr.DefaultCompressor, &decodedArchive.Data)
			_, e := xdrDecoder.ReadFrom(&capturedBuf)
			s.Require().NoError(e)

			s.Require().Equal(key, capturedKey)
			s.Require().Equal(archive.Data, decodedArchive.Data)

			// overwrite=true => "", else "true"/"false"
			expectedAlreadyExists := ""
			if !c.overwrite {
				expectedAlreadyExists = strconv.FormatBool(c.alreadyExists)
			}

			assertMetrics := func(ledgers string, compressor string, compressedBytes int, uncompressedBytes int,
				alreadyExistsLabel string) {
				m, err := uploader.uploadDurationMetric.MetricVec.GetMetricWith(
					prometheus.Labels{
						"ledgers":        ledgers,
						"already_exists": alreadyExistsLabel,
					},
				)
				s.Require().NoError(err)
				s.Require().Equal(uint64(1), getMetricValue(m).GetSummary().GetSampleCount())
				s.Require().Positive(getMetricValue(m).GetSummary().GetSampleSum())

				if alreadyExistsLabel != "" {
					opp := strconv.FormatBool(!c.alreadyExists)
					m, err = uploader.uploadDurationMetric.MetricVec.GetMetricWith(
						prometheus.Labels{
							"ledgers":        ledgers,
							"already_exists": opp,
						},
					)
					s.Require().NoError(err)
					s.Require().Equal(uint64(0), getMetricValue(m).GetSummary().GetSampleCount())
				}

				m, err = uploader.objectSizeMetrics.MetricVec.GetMetricWith(
					prometheus.Labels{
						"ledgers":        ledgers,
						"compression":    compressor,
						"already_exists": alreadyExistsLabel,
					},
				)
				s.Require().NoError(err)
				s.Require().Equal(uint64(1), getMetricValue(m).GetSummary().GetSampleCount())
				s.Require().Equal(float64(compressedBytes), getMetricValue(m).GetSummary().GetSampleSum())

				if alreadyExistsLabel != "" {
					opp := strconv.FormatBool(!c.alreadyExists)
					m, err = uploader.objectSizeMetrics.MetricVec.GetMetricWith(
						prometheus.Labels{
							"ledgers":        ledgers,
							"compression":    compressor,
							"already_exists": opp,
						},
					)
					s.Require().NoError(err)
					s.Require().Equal(uint64(0), getMetricValue(m).GetSummary().GetSampleCount())
				}

				m, err = uploader.objectSizeMetrics.MetricVec.GetMetricWith(
					prometheus.Labels{
						"ledgers":        ledgers,
						"compression":    "none",
						"already_exists": alreadyExistsLabel,
					},
				)
				s.Require().NoError(err)
				s.Require().Equal(uint64(1), getMetricValue(m).GetSummary().GetSampleCount())
				s.Require().Equal(float64(uncompressedBytes), getMetricValue(m).GetSummary().GetSampleSum())

				if alreadyExistsLabel != "" {
					opp := strconv.FormatBool(!c.alreadyExists)
					m, err = uploader.objectSizeMetrics.MetricVec.GetMetricWith(
						prometheus.Labels{
							"ledgers":        ledgers,
							"compression":    "none",
							"already_exists": opp,
						},
					)
					s.Require().NoError(err)
					s.Require().Equal(uint64(0), getMetricValue(m).GetSummary().GetSampleCount())
				}
			}

			uncompressedPayload, err := decodedArchive.Data.MarshalBinary()
			s.Require().NoError(err)

			assertMetrics(
				"100",
				xdrDecoder.Compressor.Name(),
				expectedCompressedLength,
				len(uncompressedPayload),
				expectedAlreadyExists,
			)

			s.Require().Equal(
				float64(end),
				getMetricValue(uploader.latestLedgerMetric).GetGauge().GetValue(),
			)
		})
	}
}

func (s *UploaderSuite) TestUploadPutError() {
	s.testUploadPutError(true)
	s.testUploadPutError(false)
}

func (s *UploaderSuite) testUploadPutError(putOkReturnVal bool) {
	key, start, end := "test-1-100", uint32(1), uint32(100)
	archive := NewLedgerMetaArchive(key, start, end)

	s.mockDataStore.On("PutFileIfNotExists", context.Background(), key,
		mock.Anything, datastore.MetaData{}.ToMap()).Return(putOkReturnVal, errors.New("error in PutFileIfNotExists")).Once()

	registry := prometheus.NewRegistry()
	queue := NewUploadQueue(1, registry)
	dataUploader := NewUploader(&s.mockDataStore, queue, registry, false)
	err := dataUploader.Upload(context.Background(), archive)
	s.Require().Equal(fmt.Sprintf("error uploading %s: error in PutFileIfNotExists", key), err.Error())

	for _, alreadyExists := range []string{"true", "false"} {
		metric, err := dataUploader.uploadDurationMetric.MetricVec.GetMetricWith(prometheus.Labels{
			"ledgers":        "100",
			"already_exists": alreadyExists,
		})
		s.Require().NoError(err)
		s.Require().Equal(
			uint64(0),
			getMetricValue(metric).GetSummary().GetSampleCount(),
		)

		for _, compression := range []string{compressxdr.DefaultCompressor.Name(), "none"} {
			metric, err = dataUploader.objectSizeMetrics.MetricVec.GetMetricWith(prometheus.Labels{
				"ledgers":        "100",
				"compression":    compression,
				"already_exists": alreadyExists,
			})
			s.Require().NoError(err)
			s.Require().Equal(
				uint64(0),
				getMetricValue(metric).GetSummary().GetSampleCount(),
			)
		}

		s.Require().Equal(
			float64(0),
			getMetricValue(dataUploader.latestLedgerMetric).GetGauge().GetValue(),
		)
	}
}

func (s *UploaderSuite) TestUploadPutReplaceError() {
	key, start, end := "test-1-100", uint32(1), uint32(100)
	archive := NewLedgerMetaArchive(key, start, end)

	s.mockDataStore.On("PutFile", context.Background(), key,
		mock.Anything, datastore.MetaData{}.ToMap()).Return(errors.New("error in PutFile")).Once()

	registry := prometheus.NewRegistry()
	queue := NewUploadQueue(1, registry)
	dataUploader := NewUploader(&s.mockDataStore, queue, registry, true)
	err := dataUploader.Upload(context.Background(), archive)
	s.Require().Equal(fmt.Sprintf("error uploading %s (overwrite): error in PutFile", key), err.Error())

	for _, alreadyExists := range []string{"true", "false"} {
		metric, err := dataUploader.uploadDurationMetric.MetricVec.GetMetricWith(prometheus.Labels{
			"ledgers":        "100",
			"already_exists": alreadyExists,
		})
		s.Require().NoError(err)
		s.Require().Equal(
			uint64(0),
			getMetricValue(metric).GetSummary().GetSampleCount(),
		)

		for _, compression := range []string{compressxdr.DefaultCompressor.Name(), "none"} {
			metric, err = dataUploader.objectSizeMetrics.MetricVec.GetMetricWith(prometheus.Labels{
				"ledgers":        "100",
				"compression":    compression,
				"already_exists": alreadyExists,
			})
			s.Require().NoError(err)
			s.Require().Equal(
				uint64(0),
				getMetricValue(metric).GetSummary().GetSampleCount(),
			)
		}

		s.Require().Equal(
			float64(0),
			getMetricValue(dataUploader.latestLedgerMetric).GetGauge().GetValue(),
		)
	}
}

func (s *UploaderSuite) TestRunUntilQueueClose() {
	var prev *mock.Call
	for i := 1; i <= 100; i++ {
		key := fmt.Sprintf("test-%d", i)
		cur := s.mockDataStore.On("PutFileIfNotExists", mock.Anything,
			key, mock.Anything, mock.Anything).Return(true, nil).Once()
		if prev != nil {
			cur.NotBefore(prev)
		}
		prev = cur
	}

	registry := prometheus.NewRegistry()
	queue := NewUploadQueue(1, registry)
	go func() {
		for i := uint32(1); i <= uint32(100); i++ {
			key := fmt.Sprintf("test-%d", i)
			s.Require().NoError(queue.Enqueue(s.ctx, NewLedgerMetaArchive(key, i, i)))
		}
		queue.Close()
	}()

	dataUploader := NewUploader(&s.mockDataStore, queue, registry, false)
	s.Require().NoError(dataUploader.Run(context.Background(), testShutdownDelayTime))

	s.Require().Equal(
		float64(100),
		getMetricValue(dataUploader.latestLedgerMetric).GetGauge().GetValue(),
	)
}

func (s *UploaderSuite) TestRunContextCancel() {
	ctx, cancel := context.WithCancel(context.Background())
	registry := prometheus.NewRegistry()
	queue := NewUploadQueue(1, registry)

	first := s.mockDataStore.On("PutFileIfNotExists", mock.Anything, "test", mock.Anything, datastore.MetaData{}.ToMap()).
		Return(true, nil).Once().Run(func(args mock.Arguments) {
		cancel()
	})
	s.mockDataStore.On("PutFileIfNotExists", mock.Anything, "test1", mock.Anything, datastore.MetaData{}.ToMap()).
		Return(true, nil).Once().NotBefore(first).Run(func(args mock.Arguments) {
		ctxArg := args.Get(0).(context.Context)
		s.Require().NoError(ctxArg.Err())
	})

	go func() {
		s.Require().NoError(queue.Enqueue(s.ctx, NewLedgerMetaArchive("test", 1, 1)))
		s.Require().NoError(queue.Enqueue(s.ctx, NewLedgerMetaArchive("test1", 2, 2)))
	}()

	dataUploader := NewUploader(&s.mockDataStore, queue, registry, false)
	s.Require().EqualError(dataUploader.Run(ctx, testShutdownDelayTime), "context canceled")
	s.Require().Equal(
		float64(2),
		getMetricValue(dataUploader.latestLedgerMetric).GetGauge().GetValue(),
	)
}

func (s *UploaderSuite) TestRunUploadError() {
	registry := prometheus.NewRegistry()
	queue := NewUploadQueue(2, registry)

	s.Require().NoError(queue.Enqueue(s.ctx, NewLedgerMetaArchive("test", 1, 1)))
	s.Require().NoError(queue.Enqueue(s.ctx, NewLedgerMetaArchive("test1", 2, 2)))

	s.mockDataStore.On("PutFileIfNotExists", mock.Anything, "test",
		mock.Anything, mock.Anything).Return(false, errors.New("Put error")).Once()

	dataUploader := NewUploader(&s.mockDataStore, queue, registry, false)
	err := dataUploader.Run(context.Background(), testShutdownDelayTime)
	s.Require().Equal("error uploading test: Put error", err.Error())
}

func (s *UploaderSuite) TestRunUploadPutError() {
	registry := prometheus.NewRegistry()
	queue := NewUploadQueue(2, registry)

	s.Require().NoError(queue.Enqueue(s.ctx, NewLedgerMetaArchive("test", 1, 1)))
	s.Require().NoError(queue.Enqueue(s.ctx, NewLedgerMetaArchive("test1", 2, 2)))

	s.mockDataStore.On("PutFile", mock.Anything, "test",
		mock.Anything, mock.Anything).Return(errors.New("Put error")).Once()

	dataUploader := NewUploader(&s.mockDataStore, queue, registry, true)
	err := dataUploader.Run(context.Background(), testShutdownDelayTime)
	s.Require().Equal("error uploading test (overwrite): Put error", err.Error())
}

func NewLedgerMetaArchive(key string, startSeq uint32, endSeq uint32) *LedgerMetaArchive {
	return &LedgerMetaArchive{
		ObjectKey: key,
		Data: xdr.LedgerCloseMetaBatch{
			StartSequence: xdr.Uint32(startSeq),
			EndSequence:   xdr.Uint32(endSeq),
		},
		metaData: datastore.MetaData{},
	}
}
