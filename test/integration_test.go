package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/pkg/errors"

	"github.com/pelletier/go-toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/fsouza/fake-gcs-server/fakestorage"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/storage"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-galexie/cmd"
	galexie "github.com/stellar/stellar-galexie/internal"
	"github.com/stellar/stellar-galexie/internal/scan"
)

const (
	maxWaitForCoreStartup       = 180 * time.Second
	maxWaitForLocalStackStartup = 60 * time.Second
	coreStartupPingInterval     = time.Second
	// set the max ledger we want the standalone network to emit
	// tests then refer to ledger sequences only up to this, therefore
	// don't have to do complex waiting within test for a sequence to exist.
	waitForCoreLedgerSequence = 16
	configTemplate            = "data/integration_config_template.toml"
)

// TestGalexieGCSTestSuite runs tests with GCS backend
func TestGalexieGCSTestSuite(t *testing.T) {
	if os.Getenv("GALEXIE_INTEGRATION_TESTS_ENABLED") != "true" {
		t.Skip("skipping integration test: GALEXIE_INTEGRATION_TESTS_ENABLED not true")
	}

	galexieGCSSuite := &GalexieTestSuite{
		storageType: "GCS",
	}
	suite.Run(t, galexieGCSSuite)
}

// TestGalexieS3TestSuite runs tests with S3 backend
func TestGalexieS3TestSuite(t *testing.T) {
	if os.Getenv("GALEXIE_INTEGRATION_TESTS_ENABLED") != "true" {
		t.Skip("skipping integration test: GALEXIE_INTEGRATION_TESTS_ENABLED not true")
	}

	galexieS3Suite := &GalexieTestSuite{
		storageType: "S3",
	}
	suite.Run(t, galexieS3Suite)
}

// TestGalexieFilesystemTestSuite runs tests with Filesystem backend
func TestGalexieFilesystemTestSuite(t *testing.T) {
	if os.Getenv("GALEXIE_INTEGRATION_TESTS_ENABLED") != "true" {
		t.Skip("skipping integration test: GALEXIE_INTEGRATION_TESTS_ENABLED not true")
	}

	galexieFilesystemSuite := &GalexieTestSuite{
		storageType: "Filesystem",
	}
	suite.Run(t, galexieFilesystemSuite)
}

type GalexieTestSuite struct {
	suite.Suite
	tempConfigFile        string
	testTempDir           string
	ctx                   context.Context
	ctxStop               context.CancelFunc
	coreContainerID       string
	localStackContainerID string
	dockerCli             *client.Client
	gcsServer             *fakestorage.Server
	finishedSetup         bool
	config                galexie.Config
	storageType           string // "GCS", "S3", or "Filesystem"
}

func (s *GalexieTestSuite) filesystemDataPath() string {
	return filepath.Join(s.testTempDir, "filesystem-data")
}

func (s *GalexieTestSuite) TestScanAndFill() {
	require := s.Require()

	rootCmd := cmd.DefineCommands()

	rootCmd.SetArgs([]string{"scan-and-fill", "--start", "4", "--end", "5", "--config-file", s.tempConfigFile})
	var errWriter bytes.Buffer
	var outWriter bytes.Buffer
	rootCmd.SetErr(&errWriter)
	rootCmd.SetOut(&outWriter)
	err := rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	output := outWriter.String()
	errOutput := errWriter.String()
	s.T().Log(output)
	s.T().Log(errOutput)

	datastore, err := datastore.NewDataStore(s.ctx, s.config.DataStoreConfig)
	require.NoError(err)

	_, err = datastore.GetFile(s.ctx, "FFFFFFFF--0-9/FFFFFFFA--5.xdr."+compressxdr.DefaultCompressor.Name())
	require.NoError(err)

	lastModified, err := datastore.GetFileLastModified(s.ctx, "FFFFFFFF--0-9/FFFFFFFA--5.xdr."+compressxdr.DefaultCompressor.Name())
	require.NoError(err)

	// now run an scan-and-fill on an overlapping range, it will skip over existing ledgers
	rootCmd.SetArgs([]string{"scan-and-fill", "--start", "4", "--end", "9", "--config-file", s.tempConfigFile})
	errWriter.Reset()
	rootCmd.SetErr(&errWriter)
	outWriter.Reset()
	rootCmd.SetOut(&outWriter)
	err = rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	s.T().Log(outWriter.String())
	s.T().Log(errWriter.String())

	newLastModified, err := datastore.GetFileLastModified(s.ctx, "FFFFFFFF--0-9/FFFFFFFA--5.xdr."+compressxdr.DefaultCompressor.Name())
	require.NoError(err)
	require.Equal(lastModified, newLastModified)

	_, err = datastore.GetFile(s.ctx, "FFFFFFFF--0-9/FFFFFFF6--9.xdr."+compressxdr.DefaultCompressor.Name())
	require.NoError(err)
}

func (s *GalexieTestSuite) TestReplace() {
	require := s.Require()

	rootCmd := cmd.DefineCommands()

	rootCmd.SetArgs([]string{"scan-and-fill", "--start", "4", "--end", "5", "--config-file", s.tempConfigFile})
	var errWriter bytes.Buffer
	var outWriter bytes.Buffer
	rootCmd.SetErr(&errWriter)
	rootCmd.SetOut(&outWriter)
	err := rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	output := outWriter.String()
	errOutput := errWriter.String()
	s.T().Log(output)
	s.T().Log(errOutput)

	datastore, err := datastore.NewDataStore(s.ctx, s.config.DataStoreConfig)
	require.NoError(err)

	_, err = datastore.GetFile(s.ctx, "FFFFFFFF--0-9/FFFFFFFA--5.xdr."+compressxdr.DefaultCompressor.Name())
	require.NoError(err)

	lastModified, err := datastore.GetFileLastModified(s.ctx, "FFFFFFFF--0-9/FFFFFFFA--5.xdr."+compressxdr.DefaultCompressor.Name())
	require.NoError(err)

	// S3 timestamps have second level precision. Sleep for 1 second to ensure the new timestamp is different.
	time.Sleep(1 * time.Second)

	// now run replace on an overlapping range, it will overwrite existing ledgers
	rootCmd = cmd.DefineCommands()
	rootCmd.SetArgs([]string{"replace", "--start", "4", "--end", "9", "--config-file", s.tempConfigFile})
	errWriter.Reset()
	rootCmd.SetErr(&errWriter)
	outWriter.Reset()
	rootCmd.SetOut(&outWriter)
	err = rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	s.T().Log(outWriter.String())
	s.T().Log(errWriter.String())

	newLastModified, err := datastore.GetFileLastModified(s.ctx, "FFFFFFFF--0-9/FFFFFFFA--5.xdr."+compressxdr.DefaultCompressor.Name())
	require.NoError(err)
	require.NotEqual(lastModified, newLastModified)

	_, err = datastore.GetFile(s.ctx, "FFFFFFFF--0-9/FFFFFFF6--9.xdr."+compressxdr.DefaultCompressor.Name())
	require.NoError(err)
}

func (s *GalexieTestSuite) TestAppend() {
	require := s.Require()

	// first populate ledgers 6-7
	rootCmd := cmd.DefineCommands()
	rootCmd.SetArgs([]string{"scan-and-fill", "--start", "6", "--end", "7", "--config-file", s.tempConfigFile})
	err := rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	datastore, err := datastore.NewDataStore(s.ctx, s.config.DataStoreConfig)
	require.NoError(err)

	lastModified, err := datastore.GetFileLastModified(s.ctx, "FFFFFFFF--0-9/FFFFFFF9--6.xdr."+compressxdr.DefaultCompressor.Name())
	require.NoError(err)

	// now run an append on an overlapping range, it will resume past existing ledgers
	rootCmd.SetArgs([]string{"append", "--start", "6", "--end", "9", "--config-file", s.tempConfigFile})
	var errWriter bytes.Buffer
	var outWriter bytes.Buffer
	rootCmd.SetErr(&errWriter)
	rootCmd.SetOut(&outWriter)
	err = rootCmd.ExecuteContext(s.ctx)
	require.NoError(err)

	output := outWriter.String()
	errOutput := errWriter.String()
	s.T().Log(output)
	s.T().Log(errOutput)

	// check that the file was not modified
	newLastModified, err := datastore.GetFileLastModified(s.ctx, "FFFFFFFF--0-9/FFFFFFF9--6.xdr."+compressxdr.DefaultCompressor.Name())
	require.NoError(err)
	require.Equal(lastModified, newLastModified, "file should not be modified on append of overlapping range")

	_, err = datastore.GetFile(s.ctx, "FFFFFFFF--0-9/FFFFFFF6--9.xdr."+compressxdr.DefaultCompressor.Name())
	require.NoError(err)
}

func (s *GalexieTestSuite) TestAppendUnbounded() {
	require := s.Require()

	rootCmd := cmd.DefineCommands()
	rootCmd.SetArgs([]string{"append", "--start", "10", "--config-file", s.tempConfigFile})
	var errWriter bytes.Buffer
	var outWriter bytes.Buffer
	rootCmd.SetErr(&errWriter)
	rootCmd.SetOut(&outWriter)

	appendCtx, cancel := context.WithCancel(s.ctx)
	syn := make(chan struct{})
	defer func() { <-syn }()
	defer cancel()
	go func() {
		defer close(syn)
		require.NoError(rootCmd.ExecuteContext(appendCtx))
		output := outWriter.String()
		errOutput := errWriter.String()
		s.T().Log(output)
		s.T().Log(errOutput)
	}()

	datastore, err := datastore.NewDataStore(s.ctx, s.config.DataStoreConfig)
	require.NoError(err)

	require.EventuallyWithT(func(c *assert.CollectT) {
		// this checks every 50ms up to 180s total
		assert := assert.New(c)
		_, err = datastore.GetFile(s.ctx, "FFFFFFF5--10-19/FFFFFFF0--15.xdr."+compressxdr.DefaultCompressor.Name())
		assert.NoError(err)
	}, 180*time.Second, 50*time.Millisecond, "append unbounded did not work")
}

func (s *GalexieTestSuite) TestAppendUnboundedSequenceNumber2() {
	require := s.Require()

	rootCmd := cmd.DefineCommands()
	rootCmd.SetArgs([]string{
		"append",
		"--start", "2",
		"--config-file", s.tempConfigFile,
	})

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	cmdErrCh := make(chan error, 1)
	go func() {
		cmdErrCh <- rootCmd.ExecuteContext(ctx)
	}()

	datastore, err := datastore.NewDataStore(ctx, s.config.DataStoreConfig)
	require.NoError(err)

	require.Eventually(
		func() bool {
			_, err := datastore.GetFile(
				ctx,
				"FFFFFFFF--0-9/FFFFFFFD--2.xdr."+compressxdr.DefaultCompressor.Name(),
			)
			return err == nil
		},
		60*time.Second,
		100*time.Millisecond,
		"append unbounded did not work",
	)

	cancel()
	require.NoError(<-cmdErrCh)
}

func (s *GalexieTestSuite) TestDetectGaps() {
	require := s.Require()

	// scan-and-fill a partial range
	fillCmd := cmd.DefineCommands()
	fillCmd.SetArgs([]string{
		"scan-and-fill",
		"--start", "4",
		"--end", "8",
		"--config-file", s.tempConfigFile,
	})

	require.NoError(fillCmd.ExecuteContext(s.ctx))

	// Run detect-gaps on a wider range that includes missing ledgers
	rootCmd := cmd.DefineCommands()

	var detectOut bytes.Buffer
	var detectErr bytes.Buffer
	rootCmd.SetOut(&detectOut)
	rootCmd.SetErr(&detectErr)

	rootCmd.SetArgs([]string{
		"detect-gaps",
		"--start", "2",
		"--end", "20",
		"--config-file", s.tempConfigFile,
	})

	require.NoError(rootCmd.ExecuteContext(s.ctx))

	outputJSON := detectOut.String()
	s.T().Log("detect-gaps JSON output:\n" + outputJSON)

	var resp galexie.DetectGapsOutput
	require.NoError(json.Unmarshal([]byte(outputJSON), &resp))

	require.Equal(uint32(2), resp.ScanFrom)
	require.Equal(uint32(20), resp.ScanTo)

	// Since we only filled 4–8, we expect missing ranges before and after.
	expectedReport := scan.Report{
		Gaps: []scan.Gap{
			{
				Start: 2,
				End:   3,
			},
			{
				Start: 9,
				End:   20,
			},
		},
		TotalLedgersFound:   5,
		TotalLedgersMissing: 14,
		MinSequenceFound:    4,
		MaxSequenceFound:    8,
	}
	require.Equal(expectedReport, resp.Report)

}
func (s *GalexieTestSuite) buildConfigFromTemplate(
	t *testing.T,
	filename string,
	mutate func(cfg *toml.Tree),
) (galexie.Config, string, *toml.Tree) {
	t.Helper()

	galexieConfigTemplate, err := toml.LoadFile(configTemplate)
	if err != nil {
		t.Fatalf("unable to load config template file %v, %v", configTemplate, err)
	}

	// Base settings common to all configs
	galexieConfigTemplate.Set("stellar_core_config.stellar_core_binary_path",
		os.Getenv("GALEXIE_INTEGRATION_TESTS_CAPTIVE_CORE_BIN"))
	galexieConfigTemplate.Set("stellar_core_config.storage_path",
		filepath.Join(s.testTempDir, "captive-core"))
	galexieConfigTemplate.Set("datastore_config.type", s.storageType)

	// Set storage-specific params
	if s.storageType == "Filesystem" {
		galexieConfigTemplate.Set("datastore_config.params.destination_path",
			s.filesystemDataPath())
	}

	// Apply any per-config overrides
	if mutate != nil {
		mutate(galexieConfigTemplate)
	}

	tomlBytes, err := toml.Marshal(galexieConfigTemplate)
	if err != nil {
		t.Fatalf("unable to parse config file toml %v, %v", configTemplate, err)
	}

	var cfg galexie.Config
	if err = toml.Unmarshal(tomlBytes, &cfg); err != nil {
		t.Fatalf("unable to marshal config file toml into struct, %v", err)
	}
	cfg.DataStoreConfig.NetworkPassphrase = cfg.StellarCoreConfig.NetworkPassphrase

	configPath := filepath.Join(s.testTempDir, filename)
	if err = os.WriteFile(configPath, tomlBytes, 0o777); err != nil {
		t.Fatalf("unable to write temp config file %v, %v", configPath, err)
	}

	return cfg, configPath, galexieConfigTemplate
}

func (s *GalexieTestSuite) TestDetectGaps_WritesJSONReportToFile() {
	require := s.Require()
	t := s.T()

	// Use a config where ledgers_per_file = 8 so ranges are aligned to
	// 8-ledger file boundaries.
	_, configPath, _ := s.buildConfigFromTemplate(
		t,
		"config.schema-8.toml",
		func(tree *toml.Tree) {
			tree.Set("datastore_config.schema.ledgers_per_file", int64(8))
		},
	)

	rootCmd := cmd.DefineCommands()

	// Run a small scan-and-fill to ensure some ledgers exist.
	// With ledgers_per_file = 8, a requested range of 4–8 will be aligned
	// to the file boundary and actually fill 2–7, 8-15.
	rootCmd.SetArgs([]string{
		"scan-and-fill",
		"--start", "4",
		"--end", "8",
		"--config-file", configPath,
	})
	var errBuf, outBuf bytes.Buffer
	rootCmd.SetErr(&errBuf)
	rootCmd.SetOut(&outBuf)
	err := rootCmd.ExecuteContext(s.ctx)
	require.NoError(err, errBuf.String())

	// Now run detect-gaps over a wider range and write JSON to a file.
	tmpDir := s.T().TempDir()
	reportPath := filepath.Join(tmpDir, "gaps.json")

	rootCmd.SetArgs([]string{
		"detect-gaps",
		"--start", "2",
		"--end", "20",
		"--config-file", configPath,
		"--output-file", reportPath,
	})
	errBuf.Reset()
	outBuf.Reset()
	rootCmd.SetErr(&errBuf)
	rootCmd.SetOut(&outBuf)

	require.NoError(rootCmd.ExecuteContext(s.ctx))

	// The report file should exist.
	data, readErr := os.ReadFile(reportPath)
	require.NoError(readErr)

	// Decode JSON.
	var out galexie.DetectGapsOutput
	require.NoError(json.Unmarshal(data, &out))

	expectedReport := scan.Report{
		Gaps: []scan.Gap{{
			Start: 16,
			End:   23,
		}},
		TotalLedgersFound:   14,
		TotalLedgersMissing: 8,
		MinSequenceFound:    2,
		MaxSequenceFound:    15,
	}
	require.Equal(uint32(2), out.ScanFrom)
	require.Equal(uint32(20), out.ScanTo)
	require.Equal(expectedReport, out.Report)
}

func (s *GalexieTestSuite) SetupTest() {
	t := s.T()

	if s.storageType == "GCS" {
		s.setupGCS(t)
	} else if s.storageType == "S3" {
		s.setupS3(t)
	} else if s.storageType == "Filesystem" {
		s.setupFilesystem(t)
	}
}

func (s *GalexieTestSuite) TearDownTest() {
	if s.storageType == "GCS" && s.gcsServer != nil {
		s.gcsServer.Stop()
		s.gcsServer = nil
	} else if s.storageType == "S3" && s.localStackContainerID != "" {
		s.T().Logf("Stopping the localstack container %v", s.localStackContainerID)
		s.stopAndLogContainer(s.localStackContainerID, "localstack")
		s.localStackContainerID = ""
	} else if s.storageType == "Filesystem" {
		// Clean up filesystem data between tests
		if err := os.RemoveAll(s.filesystemDataPath()); err != nil {
			s.T().Logf("Failed to clean up filesystem data path: %v", err)
		}
	}
}

func (s *GalexieTestSuite) TestIngestionLoadInvalidRange() {
	require := s.Require()
	ledgersFilePath := s.getLoadTestDataFile()
	rootCmd := cmd.DefineCommands()

	// Set up the load-test command with required flags
	rootCmd.SetArgs([]string{
		"load-test",
		"--start=8",
		"--end=12",
		"--ledgers-path=" + ledgersFilePath,
		"--close-duration=2.0", // Fast execution for testing
		"--config-file=" + s.tempConfigFile,
	})

	// Run the load test command
	err := rootCmd.Execute()
	require.ErrorContains(err, "the range of ledgers between start and end of 4 must not exceed the number of ledgers in ledgers-path file of 3")
}

func (s *GalexieTestSuite) TestIngestionLoadBoundedCmd() {
	require := s.Require()
	ledgersFilePath := s.getLoadTestDataFile()
	rootCmd := cmd.DefineCommands()

	// Set up the load-test command with required flags
	rootCmd.SetArgs([]string{
		"load-test",
		"--start=8",
		"--end=10",
		"--merge=true",
		"--ledgers-path=" + ledgersFilePath,
		"--close-duration=2.0", // Fast execution for testing
		"--config-file=" + s.tempConfigFile,
	})

	var errWriter bytes.Buffer
	var outWriter bytes.Buffer
	rootCmd.SetErr(&errWriter)
	rootCmd.SetOut(&outWriter)

	loadTestCtx, loadTestCancel := context.WithCancel(s.ctx)
	defer loadTestCancel()

	// Run the load test command
	err := rootCmd.ExecuteContext(loadTestCtx)
	require.NoError(err, "Load test command should complete the bounded range without error")

	output := outWriter.String()
	errOutput := errWriter.String()
	s.T().Log("Load test output:", output)
	s.T().Log("Load test errors:", errOutput)

	// The load test should have generated exported ledger files to datastore
	// for a total of 2 ledgers as the test ledgers fixture file contains 2 ledgers and we specifed start ledger 8
	ledgerRange := ledgerbackend.BoundedRange(uint32(8), uint32(10))
	pubConfig := ingest.PublisherConfig{
		DataStoreConfig:       s.config.DataStoreConfig,
		BufferedStorageConfig: ingest.DefaultBufferedStorageBackendConfig(s.config.DataStoreConfig.Schema.LedgersPerFile),
	}

	appCallback := func(lcm xdr.LedgerCloseMeta) error {
		isSynthetic := false
		// any ledgers with 100 tx's means it's synthetic load test data
		switch lcm.V {
		case 1:
			isSynthetic = len(lcm.V1.TxProcessing) == 100
		case 2:
			isSynthetic = len(lcm.V2.TxProcessing) == 100
		default:
			isSynthetic = false
		}
		if isSynthetic {
			loadTestCancel()
		}
		return nil
	}

	go func() {
		ingest.ApplyLedgerMetadata(ledgerRange, pubConfig, loadTestCtx, appCallback)
	}()

	require.EventuallyWithT(func(c *assert.CollectT) {
		select {
		case <-loadTestCtx.Done():
			assert.True(c, errors.Is(loadTestCtx.Err(), context.Canceled))
		default:
			assert.Fail(c, "Load test has not completed yet")
		}
	}, 120*time.Second, time.Second, "Load test should create files in datastore")

	// now that the datastore has files,
	// verify that load test mode correctly validates this as a non-empty error case
	rootCmd = cmd.DefineCommands()

	rootCmd.SetArgs([]string{
		"load-test",
		"--start=8",
		"--end=10",
		"--ledgers-path=" + ledgersFilePath,
		"--close-duration=2.0", // Fast execution for testing
		"--config-file=" + s.tempConfigFile,
	})

	err = rootCmd.Execute()
	require.ErrorContains(err, "load test mode requires an empty datastore")
}

func (s *GalexieTestSuite) TestIngestionLoadUnBoundedCmd() {
	require := s.Require()
	ledgersFilePath := s.getLoadTestDataFile()
	rootCmd := cmd.DefineCommands()

	// Set up the load-test command with required flags
	rootCmd.SetArgs([]string{
		"load-test",
		"--start=8",
		"--end=0",
		"--merge=true",
		"--ledgers-path=" + ledgersFilePath,
		"--close-duration=2.0", // Fast execution for testing
		"--config-file=" + s.tempConfigFile,
	})

	var errWriter bytes.Buffer
	var outWriter bytes.Buffer
	rootCmd.SetErr(&errWriter)
	rootCmd.SetOut(&outWriter)

	loadTestCtx, loadTestCancel := context.WithCancel(s.ctx)
	defer loadTestCancel()

	// Run the load test command
	var loadTestErr error
	cmdFinished := make(chan struct{})
	go func() {
		defer close(cmdFinished)
		loadTestErr = rootCmd.ExecuteContext(loadTestCtx)
	}()

	output := outWriter.String()
	errOutput := errWriter.String()
	s.T().Log("Load test output:", output)
	s.T().Log("Load test errors:", errOutput)

	// The load test should have generated exported ledger files to datastore
	// for a total of 2 ledgers as the test ledgers fixture file contains 2 ledgers and we specifed start ledger 8
	ledgerRange := ledgerbackend.UnboundedRange(uint32(8))
	pubConfig := ingest.PublisherConfig{
		DataStoreConfig:       s.config.DataStoreConfig,
		BufferedStorageConfig: ingest.DefaultBufferedStorageBackendConfig(s.config.DataStoreConfig.Schema.LedgersPerFile),
	}

	appCallback := func(lcm xdr.LedgerCloseMeta) error {
		isSynthetic := false
		// any ledgers with 100 tx's means it's synthetic load test data
		switch lcm.V {
		case 1:
			isSynthetic = len(lcm.V1.TxProcessing) == 100
		case 2:
			isSynthetic = len(lcm.V2.TxProcessing) == 100
		default:
			isSynthetic = false
		}
		if isSynthetic {
			loadTestCancel()
		}
		return nil
	}

	go func() {
		ingest.ApplyLedgerMetadata(ledgerRange, pubConfig, loadTestCtx, appCallback)
	}()

	require.EventuallyWithT(func(c *assert.CollectT) {
		var successCmd bool
		var successDatastore bool
		select {
		case <-cmdFinished:
			successCmd = loadTestErr == nil
		default:
		}

		select {
		case <-loadTestCtx.Done():
			successDatastore = errors.Is(loadTestCtx.Err(), context.Canceled)
		default:
		}

		assert.True(c, successCmd && successDatastore, "Load test command should complete successfully and create files in datastore")

	}, 120*time.Second, time.Second, "Load test should create files during unbounded in datastore")
}

func (s *GalexieTestSuite) SetupSuite() {
	var err error
	t := s.T()

	s.ctx, s.ctxStop = signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)

	defer func() {
		if !s.finishedSetup {
			s.TearDownSuite()
		}
	}()

	s.testTempDir = t.TempDir()

	// Build the default config used by most tests
	cfg, configPath, galexieConfigTemplate := s.buildConfigFromTemplate(
		t,
		"config.toml",
		nil,
	)
	s.config = cfg
	s.tempConfigFile = configPath

	s.dockerCli, err = client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatalf("could not create docker client, %v", err)
	}

	quickstartImage := os.Getenv("GALEXIE_INTEGRATION_TESTS_QUICKSTART_IMAGE")
	if quickstartImage == "" {
		quickstartImage = "stellar/quickstart:testing"
	}
	pullQuickStartImage := true
	if os.Getenv("GALEXIE_INTEGRATION_TESTS_QUICKSTART_IMAGE_PULL") == "false" {
		pullQuickStartImage = false
	}

	s.mustStartCore(t, quickstartImage, pullQuickStartImage)
	s.mustWaitForCore(t, galexieConfigTemplate.GetArray("stellar_core_config.history_archive_urls").([]string),
		galexieConfigTemplate.Get("stellar_core_config.network_passphrase").(string))
	s.finishedSetup = true
}

func (s *GalexieTestSuite) getLoadTestDataFile() string {
	require := s.Require()
	// this file of synthetic ledger data should be built ahead of time using services/horizon/internal/integration/generate_ledgers_test.go
	// test will only be done for current protocol version which should be stamped on the filename.
	datapath := filepath.Join("data", "load-test-ledgers-v23-standalone.xdr.zstd")
	require.FileExists(datapath, "Test ledgers file should exist")
	return datapath
}

func (s *GalexieTestSuite) setupGCS(t *testing.T) {
	tempSeedDataPath := filepath.Join(s.testTempDir, "data")
	require.NoError(t, os.RemoveAll(tempSeedDataPath))
	require.NoError(t, os.MkdirAll(filepath.Join(tempSeedDataPath, "integration-test"), 0777))

	tempBucketPath := filepath.Join(s.testTempDir, "bucket")
	require.NoError(t, os.RemoveAll(tempBucketPath))
	require.NoError(t, os.MkdirAll(tempBucketPath, 0777))

	testWriter := &testWriter{test: t}
	opts := fakestorage.Options{
		Scheme:      "http",
		Host:        "127.0.0.1",
		Port:        uint16(0),
		Writer:      testWriter,
		Seed:        tempSeedDataPath,
		StorageRoot: tempBucketPath,
		PublicHost:  "127.0.0.1",
	}

	var err error
	s.gcsServer, err = fakestorage.NewServerWithOptions(opts)

	if err != nil {
		t.Fatalf("couldn't start the fake gcs http server %v", err)
	}

	t.Logf("fake gcs server started at %v", s.gcsServer.URL())
	t.Setenv("STORAGE_EMULATOR_HOST", s.gcsServer.URL())
}

func (s *GalexieTestSuite) setupS3(t *testing.T) {
	s.mustStartLocalStack(t)
	s.mustWaitForLocalStack(t)
	s.createLocalStackBucket(t)

	t.Setenv("AWS_ACCESS_KEY_ID", "KEY_ID")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ACCESS_KEY")
}

func (s *GalexieTestSuite) setupFilesystem(t *testing.T) {
	path := s.filesystemDataPath()
	// Clean any existing data from previous tests
	require.NoError(t, os.RemoveAll(path))
	require.NoError(t, os.MkdirAll(path, 0755))
	t.Logf("Filesystem datastore path: %s", path)
}

func (s *GalexieTestSuite) TearDownSuite() {
	t := s.T()

	if s.coreContainerID != "" {
		t.Logf("Stopping the quickstart container %v", s.coreContainerID)
		s.stopAndLogContainer(s.coreContainerID, "quickstart")
		s.coreContainerID = ""
	}

	if s.dockerCli != nil {
		s.dockerCli.Close()
		s.dockerCli = nil
	}

	if s.ctxStop != nil {
		s.ctxStop()
	}
}

func (s *GalexieTestSuite) stopAndLogContainer(containerID, containerType string) {
	if s.dockerCli == nil {
		return
	}

	containerLogs, err := s.dockerCli.ContainerLogs(s.ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err == nil {
		var errWriter bytes.Buffer
		var outWriter bytes.Buffer
		stdcopy.StdCopy(&outWriter, &errWriter, containerLogs)
		s.T().Logf("%s container stdout: %s", containerType, outWriter.String())
		s.T().Logf("%s container stderr: %s", containerType, errWriter.String())
		containerLogs.Close()
	}

	if err := s.dockerCli.ContainerStop(context.Background(), containerID, container.StopOptions{}); err != nil {
		s.T().Logf("unable to stop %s container %v: %v", containerType, containerID, err)
	}
}

func (s *GalexieTestSuite) mustStartCore(t *testing.T, quickstartImage string, pullImage bool) {
	var err error
	if pullImage {
		imgReader, imgErr := s.dockerCli.ImagePull(s.ctx, quickstartImage, image.PullOptions{})
		if imgErr != nil {
			t.Fatalf("could not pull docker image, %v, %v", quickstartImage, imgErr)
		}
		// ImagePull is asynchronous.
		// The reader needs to be read completely for the pull operation to complete.
		_, err = io.Copy(io.Discard, imgReader)
		if err != nil {
			t.Fatalf("could not pull docker image, %v, %v", quickstartImage, err)
		}

		err = imgReader.Close()
		if err != nil {
			t.Fatalf("could not download all of docker image bytes after pull, %v, %v", quickstartImage, err)
		}
	}

	resp, err := s.dockerCli.ContainerCreate(s.ctx,
		&container.Config{
			Image: quickstartImage,
			// only run tge core service(no horizon, rpc, etc) and don't spend any time upgrading
			// the core with newer soroban limits
			Cmd: []string{"--enable", "core,,", "--limits", "default", "--local"},
			ExposedPorts: nat.PortSet{
				nat.Port("1570/tcp"):  {},
				nat.Port("11625/tcp"): {},
			},
		},

		&container.HostConfig{
			PortBindings: nat.PortMap{
				nat.Port("1570/tcp"):  {nat.PortBinding{HostIP: "127.0.0.1", HostPort: "1570"}},
				nat.Port("11625/tcp"): {nat.PortBinding{HostIP: "127.0.0.1", HostPort: "11625"}},
			},
			AutoRemove: true,
		},
		nil, nil, "")

	if err != nil {
		t.Fatalf("could not create quickstart docker container, %v, error %v", quickstartImage, err)
	}
	s.coreContainerID = resp.ID

	if err := s.dockerCli.ContainerStart(s.ctx, resp.ID, container.StartOptions{}); err != nil {
		t.Fatalf("could not run quickstart docker container, %v, error %v", quickstartImage, err)
	}
	t.Logf("Started quickstart container %v", s.coreContainerID)
}

func (s *GalexieTestSuite) mustWaitForCore(t *testing.T, archiveUrls []string, passphrase string) {
	t.Log("Waiting for core to be up...")
	startTime := time.Now()
	infoTime := startTime
	archive, err := historyarchive.NewArchivePool(archiveUrls, historyarchive.ArchiveOptions{
		NetworkPassphrase: passphrase,
		// due to ARTIFICIALLY_ACCELERATE_TIME_FOR_TESTING that is done by quickstart's local network
		CheckpointFrequency: 8,
		ConnectOptions: storage.ConnectOptions{
			Context: s.ctx,
		},
	})
	if err != nil {
		t.Fatalf("unable to create archive pool against core, %v", err)
	}
	for time.Since(startTime) < maxWaitForCoreStartup {
		if durationSince := time.Since(infoTime); durationSince < coreStartupPingInterval {
			time.Sleep(coreStartupPingInterval - durationSince)
		}
		infoTime = time.Now()
		has, requestErr := archive.GetRootHAS()
		if errors.Is(requestErr, context.Canceled) {
			break
		}
		if requestErr != nil {
			t.Logf("request to fetch checkpoint failed: %v", requestErr)
			continue
		}
		latestCheckpoint := has.CurrentLedger
		if latestCheckpoint >= waitForCoreLedgerSequence {
			return
		}
	}
	t.Fatalf("core did not progress ledgers within %v seconds", maxWaitForCoreStartup)
}

func (s *GalexieTestSuite) mustStartLocalStack(t *testing.T) {
	t.Log("Starting LocalStack container...")
	imageTag := os.Getenv("GALEXIE_INTEGRATION_TESTS_LOCALSTACK_IMAGE_TAG")
	if imageTag == "" {
		imageTag = "latest"
	}
	imageName := "localstack/localstack:" + imageTag
	pullImage := os.Getenv("GALEXIE_INTEGRATION_TESTS_LOCALSTACK_IMAGE_PULL") != "false"
	if pullImage {
		imgReader, err := s.dockerCli.ImagePull(s.ctx, imageName, image.PullOptions{})
		if err != nil {
			t.Fatalf("could not pull docker image %s: %v", imageName, err)
		}
		defer imgReader.Close()
		_, err = io.Copy(io.Discard, imgReader)
		if err != nil {
			t.Fatalf("could not read docker image pull response %s: %v", imageName, err)
		}
	}

	resp, err := s.dockerCli.ContainerCreate(s.ctx,
		&container.Config{
			Image: imageName,
			ExposedPorts: nat.PortSet{
				"4566/tcp": {},
			},
		},
		&container.HostConfig{
			PortBindings: nat.PortMap{
				"4566/tcp": {nat.PortBinding{HostIP: "127.0.0.1", HostPort: "4566"}},
			},
			AutoRemove: true,
		},
		nil, nil, "")
	if err != nil {
		t.Fatalf("could not create localstack container: %v", err)
	}
	s.localStackContainerID = resp.ID

	if err := s.dockerCli.ContainerStart(s.ctx, resp.ID, container.StartOptions{}); err != nil {
		t.Fatalf("could not run localstack container: %v", err)
	}
	t.Logf("Started LocalStack container %v", s.localStackContainerID)
}

func (s *GalexieTestSuite) mustWaitForLocalStack(t *testing.T) {
	t.Log("Waiting for LocalStack to be up...")
	healthURL := "http://localhost:4566/_localstack/health"
	startTime := time.Now()

	httpClient := &http.Client{Timeout: 5 * time.Second}

	for time.Since(startTime) < maxWaitForLocalStackStartup {
		req, err := http.NewRequestWithContext(s.ctx, "GET", healthURL, nil)
		if err != nil {
			t.Logf("failed to create http request to localstack: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			t.Logf("failed to connect to localstack: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Logf("LocalStack is ready. Health check response: %s", string(body))
			return
		}
		resp.Body.Close()
		t.Logf("LocalStack health check failed with status: %s. Retrying...", resp.Status)
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("LocalStack did not become ready within %v", maxWaitForLocalStackStartup)
}

func (s *GalexieTestSuite) createLocalStackBucket(t *testing.T) {
	bucketName := "integration-test"
	url := "http://localhost:4566/" + bucketName
	req, err := http.NewRequestWithContext(s.ctx, "PUT", url, nil)
	if err != nil {
		t.Fatalf("failed to create request to create bucket: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to create bucket %s: %v", bucketName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("failed to create bucket %s: %s", bucketName, string(body))
	}
	t.Logf("Bucket %s created successfully", bucketName)
}

type testWriter struct {
	test *testing.T
}

func (w *testWriter) Write(p []byte) (n int, err error) {
	w.test.Log(string(p))
	return len(p), nil
}
