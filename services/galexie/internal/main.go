package galexie

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stellar/go/support/strutils"
)

var (
	galexieCmdRunner = func(runtimeSettings RuntimeSettings) error {
		app := NewApp()
		return app.Run(runtimeSettings)
	}
)

func Execute() error {
	rootCmd := defineCommands()
	return rootCmd.Execute()
}

func defineCommands() *cobra.Command {
	var rootCmd = &cobra.Command{
		Use:   "galexie",
		Short: "Export Stellar network ledger data to a remote data store",
		Long:  "Converts ledger meta data from Stellar network into static data and exports it remote data storage.",

		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("please specify one of the availble sub-commands to initiate export")
		},
	}
	var scanAndFillCmd = &cobra.Command{
		Use:   "scan-and-fill",
		Short: "scans the entire bounded requested range between 'start' and 'end' flags and exports only the ledgers which are missing from the data lake.",
		Long:  "scans the entire bounded requested range between 'start' and 'end' flags and exports only the ledgers which are missing from the data lake.",
		RunE: func(cmd *cobra.Command, args []string) error {
			settings := bindCliParameters(cmd.PersistentFlags().Lookup("start"),
				cmd.PersistentFlags().Lookup("end"),
				cmd.PersistentFlags().Lookup("config-file"),
			)
			settings.Mode = ScanFill
			settings.Ctx = cmd.Context()
			if settings.Ctx == nil {
				settings.Ctx = context.Background()
			}
			return galexieCmdRunner(settings)
		},
	}
	var appendCmd = &cobra.Command{
		Use:   "append",
		Short: "export ledgers beginning with the first missing ledger after the specified 'start' ledger and resumes exporting from there",
		Long:  "export ledgers beginning with the first missing ledger after the specified 'start' ledger and resumes exporting from there",
		RunE: func(cmd *cobra.Command, args []string) error {
			settings := bindCliParameters(cmd.PersistentFlags().Lookup("start"),
				cmd.PersistentFlags().Lookup("end"),
				cmd.PersistentFlags().Lookup("config-file"),
			)
			settings.Mode = Append
			settings.Ctx = cmd.Context()
			if settings.Ctx == nil {
				settings.Ctx = context.Background()
			}
			return galexieCmdRunner(settings)
		},
	}
	var ReplaceCmd = &cobra.Command{
		Use:   "replace",
		Short: "Re-exports all ledgers, replacing existing files in the data lake.",
		Long: "Performs a full re-export of all ledgers within the bounded range (defined by 'start' and 'end' flags)." +
			" This command will overwrite any existing files at the destination path within the data lake",
		RunE: func(cmd *cobra.Command, args []string) error {
			settings := bindCliParameters(cmd.PersistentFlags().Lookup("start"),
				cmd.PersistentFlags().Lookup("end"),
				cmd.PersistentFlags().Lookup("config-file"),
			)
			settings.Mode = Replace
			settings.Ctx = cmd.Context()
			if settings.Ctx == nil {
				settings.Ctx = context.Background()
			}
			return galexieCmdRunner(settings)
		},
	}

	var loadTestCmd = &cobra.Command{
		Use: "load-test",
		Short: "runs an ingestion load test for galexie. the range of ledgers to be processed " +
			"during load test is determined as the specified start and end.  For bounded ranges " +
			"there must be at least that many ledgers in ledgers-path, for unbounded the load test will stop after it replays all ledgers from ledgers-path",
		RunE: func(cmd *cobra.Command, args []string) error {
			settings := bindLoadTestCliParameters(
				cmd.PersistentFlags().Lookup("start"),
				cmd.PersistentFlags().Lookup("end"),
				cmd.PersistentFlags().Lookup("merge"),
				cmd.PersistentFlags().Lookup("ledgers-path"),
				cmd.PersistentFlags().Lookup("close-duration"),
				cmd.PersistentFlags().Lookup("config-file"),
			)
			settings.Mode = LoadTest
			settings.Ctx = cmd.Context()
			if settings.Ctx == nil {
				settings.Ctx = context.Background()
			}
			return galexieCmdRunner(settings)
		},
	}

	rootCmd.AddCommand(scanAndFillCmd)
	rootCmd.AddCommand(appendCmd)
	rootCmd.AddCommand(ReplaceCmd)
	rootCmd.AddCommand(loadTestCmd)

	commonFlags := pflag.NewFlagSet("common_flags", pflag.ExitOnError)
	commonFlags.Uint32P("start", "s", 0, "Starting ledger (inclusive), must be set to a value greater than 1")
	commonFlags.Uint32P("end", "e", 0, "Ending ledger (inclusive), must be set to value greater than 'start' and less than the network's current ledger")
	commonFlags.String("config-file", "config.toml", "Path to the TOML config file. Defaults to 'config.toml' on runtime working directory path.")

	scanAndFillCmd.PersistentFlags().AddFlagSet(commonFlags)
	viper.BindPFlags(scanAndFillCmd.PersistentFlags())

	ReplaceCmd.PersistentFlags().AddFlagSet(commonFlags)
	viper.BindPFlags(ReplaceCmd.PersistentFlags())

	appendCmd.PersistentFlags().Uint32P("start", "s", 0, "Starting ledger (inclusive), must be set to a value greater than 1")
	appendCmd.PersistentFlags().Uint32P("end", "e", 0, "Ending ledger (inclusive), optional, setting to non-zero means bounded mode, "+
		"only export ledgers from 'start' up to 'end' value which must be greater than 'start' and less than the network's current ledger. "+
		"If 'end' is absent or '0' means unbounded mode, exporter will continue to run indefintely and export the latest closed ledgers from network as they are generated in real time.")
	appendCmd.PersistentFlags().String("config-file", "config.toml", "Path to the TOML config file. Defaults to 'config.toml' on runtime working directory path.")
	viper.BindPFlags(appendCmd.PersistentFlags())

	loadTestCmd.PersistentFlags().Uint32P("start", "s", 0, "Starting ledger (inclusive). load test will use as the starting point from live network upon which synthetic ledger changes are generated. Must be greater than 1")
	loadTestCmd.PersistentFlags().Uint32P("end", "e", 0, "Ending ledger (inclusive), optional. must be greater than 'start' if present. If 'end' is absent or set to '0' load test will replay all ledgers in ledgers-path file. otherwise load test will stop after reaching 'end' ledger regardless of any additional ledgers in ledgers-path file.")
	loadTestCmd.PersistentFlags().Bool("merge", false, "whether to merge ledger entries from real ledger backend into the ingestion load test.")
	loadTestCmd.PersistentFlags().String("ledgers-path", "", "path to ledgers file which will be replayed in the ingestion load test.")
	loadTestCmd.PersistentFlags().Float64("close-duration", 2.0, "the time (in seconds) it takes to close ledgers in the ingestion load test.")
	loadTestCmd.PersistentFlags().String("config-file", "config.toml", "Path to the TOML config file. Defaults to 'config.toml' on runtime working directory path.")
	viper.BindPFlags(loadTestCmd.PersistentFlags())

	return rootCmd
}

func bindCliParameters(startFlag *pflag.Flag, endFlag *pflag.Flag, configFileFlag *pflag.Flag) RuntimeSettings {
	settings := RuntimeSettings{}

	viper.BindPFlag(startFlag.Name, startFlag)
	viper.BindEnv(startFlag.Name, strutils.KebabToConstantCase(startFlag.Name))
	settings.StartLedger = viper.GetUint32(startFlag.Name)

	viper.BindPFlag(endFlag.Name, endFlag)
	viper.BindEnv(endFlag.Name, strutils.KebabToConstantCase(endFlag.Name))
	settings.EndLedger = viper.GetUint32(endFlag.Name)

	viper.BindPFlag(configFileFlag.Name, configFileFlag)
	viper.BindEnv(configFileFlag.Name, strutils.KebabToConstantCase(configFileFlag.Name))
	settings.ConfigFilePath = viper.GetString(configFileFlag.Name)

	return settings
}

func bindLoadTestCliParameters(startFlag *pflag.Flag, endFlag *pflag.Flag, mergeFlag *pflag.Flag, ledgersPathFlag *pflag.Flag, closeDurationFlag *pflag.Flag, configFileFlag *pflag.Flag) RuntimeSettings {
	settings := RuntimeSettings{}

	viper.BindPFlag(startFlag.Name, startFlag)
	viper.BindEnv(startFlag.Name, strutils.KebabToConstantCase(startFlag.Name))
	settings.StartLedger = viper.GetUint32(startFlag.Name)

	viper.BindPFlag(endFlag.Name, endFlag)
	viper.BindEnv(endFlag.Name, strutils.KebabToConstantCase(endFlag.Name))
	settings.EndLedger = viper.GetUint32(endFlag.Name)

	viper.BindPFlag(mergeFlag.Name, mergeFlag)
	viper.BindEnv(mergeFlag.Name, strutils.KebabToConstantCase(mergeFlag.Name))
	settings.LoadTestMerge = viper.GetBool(mergeFlag.Name)

	viper.BindPFlag(ledgersPathFlag.Name, ledgersPathFlag)
	viper.BindEnv(ledgersPathFlag.Name, strutils.KebabToConstantCase(ledgersPathFlag.Name))
	settings.LoadTestLedgersPath = viper.GetString(ledgersPathFlag.Name)

	viper.BindPFlag(closeDurationFlag.Name, closeDurationFlag)
	viper.BindEnv(closeDurationFlag.Name, strutils.KebabToConstantCase(closeDurationFlag.Name))
	seconds := viper.GetFloat64(closeDurationFlag.Name)
	settings.LoadTestCloseDuration = time.Duration(seconds * float64(time.Second))

	viper.BindPFlag(configFileFlag.Name, configFileFlag)
	viper.BindEnv(configFileFlag.Name, strutils.KebabToConstantCase(configFileFlag.Name))
	settings.ConfigFilePath = viper.GetString(configFileFlag.Name)

	return settings
}
