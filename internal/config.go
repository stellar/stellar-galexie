package galexie

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/pelletier/go-toml"

	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/storage"
)

const (
	Pubnet    = "pubnet"
	Testnet   = "testnet"
	UserAgent = "galexie"
)

type Mode int

const (
	_        Mode = iota
	ScanFill Mode = iota
	Append
	Replace
	LoadTest
)

func (mode Mode) Name() string {
	switch mode {
	case ScanFill:
		return "Scan and Fill"
	case Append:
		return "Append"
	case Replace:
		return "Replace"
	case LoadTest:
		return "Load Test"
	}
	return "none"
}

// user-configurable in the future.
const compressionType = "zstd"

type RuntimeSettings struct {
	StartLedger    uint32
	EndLedger      uint32
	ConfigFilePath string
	Mode           Mode
	Ctx            context.Context
	// Load test specific fields
	LoadTestMerge         bool
	LoadTestLedgersPath   string
	LoadTestCloseDuration time.Duration
}

type StellarCoreConfig struct {
	Network               string   `toml:"network"`
	NetworkPassphrase     string   `toml:"network_passphrase"`
	HistoryArchiveUrls    []string `toml:"history_archive_urls"`
	StellarCoreBinaryPath string   `toml:"stellar_core_binary_path"`
	CaptiveCoreTomlPath   string   `toml:"captive_core_toml_path"`
	CheckpointFrequency   uint32   `toml:"checkpoint_frequency"`
	StoragePath           string   `toml:"storage_path"`
}

type Config struct {
	AdminPort int `toml:"admin_port"`

	DataStoreConfig   datastore.DataStoreConfig `toml:"datastore_config"`
	StellarCoreConfig StellarCoreConfig         `toml:"stellar_core_config"`
	UserAgent         string                    `toml:"user_agent"`

	StartLedger uint32
	EndLedger   uint32
	Mode        Mode

	// Load test specific fields
	LoadTestLedgersPath   string
	LoadTestCloseDuration time.Duration
	LoadTestMerge         bool

	CoreVersion               string
	SerializedCaptiveCoreToml []byte
	CoreBuildVersionFn        ledgerbackend.CoreBuildVersionFunc
}

// This will generate the config based on settings
//
// settings              - requested settings
//
// return                - *Config or an error if any range validation failed.
func NewConfig(settings RuntimeSettings, getCoreVersionFn ledgerbackend.CoreBuildVersionFunc) (*Config, error) {
	config := &Config{}

	config.StartLedger = uint32(settings.StartLedger)
	config.EndLedger = uint32(settings.EndLedger)
	config.Mode = settings.Mode
	config.LoadTestMerge = settings.LoadTestMerge
	config.LoadTestLedgersPath = settings.LoadTestLedgersPath
	config.LoadTestCloseDuration = settings.LoadTestCloseDuration
	config.CoreBuildVersionFn = ledgerbackend.CoreBuildVersion
	if getCoreVersionFn != nil {
		config.CoreBuildVersionFn = getCoreVersionFn
	}

	logger.Infof("Requested export mode of %v with start=%d, end=%d", settings.Mode.Name(), config.StartLedger, config.EndLedger)

	var err error
	if err = config.processToml(settings.ConfigFilePath); err != nil {
		return nil, err
	}
	logger.Infof("Network Config Archive URLs: %v", config.StellarCoreConfig.HistoryArchiveUrls)
	logger.Infof("Network Config Archive Passphrase: %v", config.StellarCoreConfig.NetworkPassphrase)
	logger.Infof("Network Config Stellar Core Binary Path: %v", config.StellarCoreConfig.StellarCoreBinaryPath)
	logger.Infof("Network Config Stellar Core Toml Config: %v", string(config.SerializedCaptiveCoreToml))

	return config, nil
}

func (config *Config) Resumable() bool {
	return config.Mode == Append
}

// Validates requested ledger range, and will automatically adjust it
// to be ledgers-per-file boundary aligned
func (config *Config) ValidateAndSetLedgerRange(ctx context.Context, archive historyarchive.ArchiveInterface) error {

	if config.Mode == LoadTest {
		if config.LoadTestLedgersPath == "" {
			return errors.New("ledgers-path is required for load test mode")
		}

		// configure load-test as a bounded range starting from current network ledger and including up to
		// ledgers available from fixtures files
		ledgersCount, err := countLoadTestLedgers(config.LoadTestLedgersPath)
		if err != nil {
			return errors.Wrap(err, "failed to count ledgers in ledgers-path file for load test mode")
		}
		if ledgersCount == 0 {
			return errors.New("ledgers-path file contains no ledgers for load test mode")
		}

		if config.EndLedger != 0 {
			if config.EndLedger-config.StartLedger > ledgersCount {
				return errors.Errorf("invalid end value, the range of ledgers between start and end of %d must not exceed the number of ledgers in ledgers-path file of %d", config.EndLedger-config.StartLedger, ledgersCount)
			}
			logger.Infof("Detected %d ledgers in load test ledgers-path file, setting the load test mode ledger range to %d-%d", ledgersCount, config.StartLedger, config.EndLedger)
		}

		return nil
	}

	if config.StartLedger < 2 {
		return errors.New("invalid start value, must be greater than one.")
	}

	if (config.Mode == ScanFill || config.Mode == Replace) && config.EndLedger == 0 {
		return errors.New("invalid end value, unbounded mode not supported, end must be greater than start.")
	}

	if config.EndLedger != 0 && config.EndLedger <= config.StartLedger {
		return errors.New("invalid end value, must be greater than start")
	}

	latestNetworkLedger, err := archive.GetLatestLedgerSequence()
	latestNetworkLedger = latestNetworkLedger + (archive.GetCheckpointManager().GetCheckpointFrequency() * 2)

	if err != nil {
		return errors.Wrap(err, "Failed to retrieve the latest ledger sequence from history archives.")
	}
	logger.Infof("Latest ledger sequence was detected as %d", latestNetworkLedger)

	if config.StartLedger > latestNetworkLedger {
		return errors.Errorf("start %d exceeds latest network ledger %d",
			config.StartLedger, latestNetworkLedger)
	}

	if config.EndLedger > latestNetworkLedger {
		return errors.Errorf("end %d exceeds latest network ledger %d",
			config.EndLedger, latestNetworkLedger)
	}

	config.adjustLedgerRange()
	return nil
}

func (config *Config) GenerateHistoryArchive(ctx context.Context, entry *log.Entry) (historyarchive.ArchiveInterface, error) {
	return historyarchive.NewArchivePool(config.StellarCoreConfig.HistoryArchiveUrls, historyarchive.ArchiveOptions{
		ConnectOptions: storage.ConnectOptions{
			UserAgent: config.UserAgent,
			Context:   ctx,
		},
		Logger: logger,
	})
}

// coreBinDefaultPath - a default value to use for core binary path on system.
//
//	this will be used if StellarCoreConfig.StellarCoreBinaryPath is not specified
func (config *Config) GenerateCaptiveCoreConfig(coreBinFromPath string) (ledgerbackend.CaptiveCoreConfig, error) {
	var err error

	if config.StellarCoreConfig.StellarCoreBinaryPath == "" && coreBinFromPath == "" {
		return ledgerbackend.CaptiveCoreConfig{}, errors.New("Invalid captive core config, no stellar-core binary path was provided.")
	}

	if config.StellarCoreConfig.StellarCoreBinaryPath == "" {
		config.StellarCoreConfig.StellarCoreBinaryPath = coreBinFromPath
	}

	if err = config.setCoreVersionInfo(); err != nil {
		return ledgerbackend.CaptiveCoreConfig{}, fmt.Errorf("failed to set stellar-core version info: %w", err)
	}

	params := ledgerbackend.CaptiveCoreTomlParams{
		CoreBinaryPath:     config.StellarCoreConfig.StellarCoreBinaryPath,
		NetworkPassphrase:  config.StellarCoreConfig.NetworkPassphrase,
		HistoryArchiveURLs: config.StellarCoreConfig.HistoryArchiveUrls,
		EmitVerboseMeta:    true,
	}

	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreTomlFromData(config.SerializedCaptiveCoreToml, params)
	if err != nil {
		return ledgerbackend.CaptiveCoreConfig{}, errors.Wrap(err, "Failed to create captive-core toml")
	}

	checkpointFrequency := historyarchive.DefaultCheckpointFrequency
	if config.StellarCoreConfig.CheckpointFrequency > 0 {
		checkpointFrequency = config.StellarCoreConfig.CheckpointFrequency
	}
	return ledgerbackend.CaptiveCoreConfig{
		BinaryPath:          config.StellarCoreConfig.StellarCoreBinaryPath,
		NetworkPassphrase:   params.NetworkPassphrase,
		HistoryArchiveURLs:  params.HistoryArchiveURLs,
		CheckpointFrequency: checkpointFrequency,
		Log:                 logger.WithField("subservice", "stellar-core"),
		Toml:                captiveCoreToml,
		UserAgent:           config.UserAgent,
		StoragePath:         config.StellarCoreConfig.StoragePath,
	}, nil
}

func (c *Config) setCoreVersionInfo() (err error) {
	c.CoreVersion, err = c.CoreBuildVersionFn(c.StellarCoreConfig.StellarCoreBinaryPath)
	if err != nil {
		return fmt.Errorf("failed to set stellar-core version: %w", err)
	}
	logger.Infof("stellar-core version: %s", c.CoreVersion)
	return nil
}

func (config *Config) processToml(tomlPath string) error {
	// Load config TOML file
	cfg, err := toml.LoadFile(tomlPath)
	if err != nil {
		return errors.Wrapf(err, "config file %v was not found", tomlPath)
	}

	// Unmarshal TOML data into the Config struct
	if err = cfg.Unmarshal(config); err != nil {
		return errors.Wrap(err, "Error unmarshalling TOML config.")
	}

	if config.UserAgent == "" {
		config.UserAgent = UserAgent
	}

	if config.StellarCoreConfig.Network == "" && (len(config.StellarCoreConfig.HistoryArchiveUrls) == 0 || config.StellarCoreConfig.NetworkPassphrase == "" || config.StellarCoreConfig.CaptiveCoreTomlPath == "") {
		return errors.New("Invalid captive core config, the 'network' parameter must be set to pubnet or testnet or " +
			"'stellar_core_config.history_archive_urls' and 'stellar_core_config.network_passphrase' and 'stellar_core_config.captive_core_toml_path' must be set.")
	}

	// network config values are an overlay, with network preconfigured values being first if network is present
	// and then toml settings specific for passphrase, archiveurls, core toml file can override lastly.
	var networkPassPhrase string
	var networkArchiveUrls []string
	switch config.StellarCoreConfig.Network {
	case "":

	case Pubnet:
		networkPassPhrase = network.PublicNetworkPassphrase
		networkArchiveUrls = network.PublicNetworkhistoryArchiveURLs
		config.SerializedCaptiveCoreToml = ledgerbackend.PubnetDefaultConfig

	case Testnet:
		networkPassPhrase = network.TestNetworkPassphrase
		networkArchiveUrls = network.TestNetworkhistoryArchiveURLs
		config.SerializedCaptiveCoreToml = ledgerbackend.TestnetDefaultConfig

	default:
		return errors.New("invalid captive core config, " +
			"preconfigured_network must be set to 'pubnet' or 'testnet' or network_passphrase, history_archive_urls," +
			" and captive_core_toml_path must be set")
	}

	if config.StellarCoreConfig.NetworkPassphrase == "" {
		config.StellarCoreConfig.NetworkPassphrase = networkPassPhrase
	}

	if len(config.StellarCoreConfig.HistoryArchiveUrls) < 1 {
		config.StellarCoreConfig.HistoryArchiveUrls = networkArchiveUrls
	}

	if config.StellarCoreConfig.CaptiveCoreTomlPath != "" {
		if config.SerializedCaptiveCoreToml, err = os.ReadFile(config.StellarCoreConfig.CaptiveCoreTomlPath); err != nil {
			return errors.Wrap(err, "Failed to load captive-core-toml-path file")
		}
	}

	// Populate the datastore config with the network passphrase for datastore manifest.
	config.DataStoreConfig.NetworkPassphrase = config.StellarCoreConfig.NetworkPassphrase
	config.DataStoreConfig.Compression = compressionType

	return nil
}

func (config *Config) adjustLedgerRange() {
	// Check if either the start or end ledger does not fall on the "LedgersPerFile" boundary
	// and adjust the start and end ledger accordingly.
	// Align the start ledger to the nearest "LedgersPerFile" boundary.
	config.StartLedger = config.DataStoreConfig.Schema.GetSequenceNumberStartBoundary(config.StartLedger)

	// Ensure that the adjusted start ledger is at least 2.
	config.StartLedger = max(2, config.StartLedger)

	// Align the end ledger (for bounded cases) to the nearest "LedgersPerFile" boundary.
	if config.EndLedger != 0 {
		config.EndLedger = config.DataStoreConfig.Schema.GetSequenceNumberEndBoundary(config.EndLedger)
	}

	logger.Infof("Computed effective export boundary ledger range: start=%d, end=%d", config.StartLedger, config.EndLedger)
}

func countLoadTestLedgers(path string) (uint32, error) {
	ledgersCount := uint32(0)
	file, err := os.Open(path)
	if err != nil {
		return ledgersCount, err
	}

	stream, err := xdr.NewZstdStream(file)
	if err != nil {
		return ledgersCount, err
	}

	defer func() {
		if closeErr := stream.Close(); closeErr != nil {
			logger.WithError(closeErr).Warn("Failed to close load test ledgers file stream")
		}
	}()

	for {
		var lcm xdr.LedgerCloseMeta
		if err = stream.ReadOne(&lcm); err == io.EOF {
			break
		}
		if err != nil {
			return ledgersCount, err
		}
		ledgersCount++
	}

	return ledgersCount, nil
}
