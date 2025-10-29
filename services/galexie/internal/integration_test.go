package galexie

import (
	"bytes"
	"context"
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

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/compressxdr"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/storage"
	"github.com/stellar/go/xdr"
)

const (
	maxWaitForCoreStartup       = 180 * time.Second
	maxWaitForLocalStackStartup = 60 * time.Second
	coreStartupPingInterval     = time.Second
	// set the max ledger we want the standalone network to emit
	// tests then refer to ledger sequences only up to this, therefore
	// don't have to do complex waiting within test for a sequence to exist.
	waitForCoreLedgerSequence = 16
	configTemplate            = "test/integration_config_template.toml"
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
	config                Config
	storageType           string // "GCS" or "S3"
}

func (s *GalexieTestSuite) TestScanAndFill() {
	require := s.Require()

	rootCmd := defineCommands()

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

func (s *GalexieTestSuite) TesReplace() {
	require := s.Require()

	rootCmd := defineCommands()

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
	rootCmd = defineCommands()
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
	rootCmd := defineCommands()
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

	rootCmd := defineCommands()
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

func (s *GalexieTestSuite) SetupTest() {
	t := s.T()

	if s.storageType == "GCS" {
		s.setupGCS(t)
	} else if s.storageType == "S3" {
		s.setupS3(t)
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
	}
}

func (s *GalexieTestSuite) TestIngestionLoadInvalidRange() {
	require := s.Require()
	ledgersFilePath := s.getLoadTestDataFile()
	rootCmd := defineCommands()

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
	rootCmd := defineCommands()

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
	rootCmd = defineCommands()

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
	rootCmd := defineCommands()

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

	galexieConfigTemplate, err := toml.LoadFile(configTemplate)
	if err != nil {
		t.Fatalf("unable to load config template file %v, %v", configTemplate, err)
	}

	// if GALEXIE_INTEGRATION_TESTS_CAPTIVE_CORE_BIN not specified,
	// galexie will attempt resolve core bin using 'stellar-core' from OS path
	galexieConfigTemplate.Set("stellar_core_config.stellar_core_binary_path",
		os.Getenv("GALEXIE_INTEGRATION_TESTS_CAPTIVE_CORE_BIN"))

	galexieConfigTemplate.Set("stellar_core_config.storage_path", filepath.Join(s.testTempDir, "captive-core"))
	galexieConfigTemplate.Set("datastore_config.type", s.storageType)

	tomlBytes, err := toml.Marshal(galexieConfigTemplate)
	if err != nil {
		t.Fatalf("unable to parse config file toml %v, %v", configTemplate, err)
	}
	if err = toml.Unmarshal(tomlBytes, &s.config); err != nil {
		t.Fatalf("unable to marshal config file toml into struct, %v", err)
	}
	s.config.DataStoreConfig.NetworkPassphrase = s.config.StellarCoreConfig.NetworkPassphrase

	s.tempConfigFile = filepath.Join(s.testTempDir, "config.toml")
	err = os.WriteFile(s.tempConfigFile, tomlBytes, 0777)
	if err != nil {
		t.Fatalf("unable to write temp config file %v, %v", s.tempConfigFile, err)
	}

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
	datapath := filepath.Join("testdata", "load-test-ledgers-v23-standalone.xdr.zstd")
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
