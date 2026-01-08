package cmd

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	galexie "github.com/stellar/stellar-galexie/internal"
)

func TestFlagsOutput(t *testing.T) {
	var testResultSettings galexie.RuntimeSettings
	appRunnerSuccess := func(runtimeSettings galexie.RuntimeSettings) error {
		testResultSettings = runtimeSettings
		return nil
	}

	appRunnerError := func(runtimeSettings galexie.RuntimeSettings) error {
		return errors.New("test error")
	}

	ctx := context.Background()

	testCases := []struct {
		name              string
		commandArgs       []string
		expectedErrOutput string
		appRunner         func(runtimeSettings galexie.RuntimeSettings) error
		expectedSettings  galexie.RuntimeSettings
	}{
		{
			name:              "no sub-command",
			commandArgs:       []string{"--start", "4", "--end", "5", "--config-file", "myfile"},
			expectedErrOutput: "Error: ",
		},
		{
			name:              "append sub-command with start and end present",
			commandArgs:       []string{"append", "--start", "4", "--end", "5", "--config-file", "myfile"},
			expectedErrOutput: "",
			appRunner:         appRunnerSuccess,
			expectedSettings: galexie.RuntimeSettings{
				StartLedger:    4,
				EndLedger:      5,
				ConfigFilePath: "myfile",
				Mode:           galexie.Append,
				Ctx:            ctx,
			},
		},
		{
			name:              "append sub-command with start and end absent",
			commandArgs:       []string{"append", "--config-file", "myfile"},
			expectedErrOutput: "",
			appRunner:         appRunnerSuccess,
			expectedSettings: galexie.RuntimeSettings{
				StartLedger:    0,
				EndLedger:      0,
				ConfigFilePath: "myfile",
				Mode:           galexie.Append,
				Ctx:            ctx,
			},
		},
		{
			name:              "append sub-command prints app error",
			commandArgs:       []string{"append", "--start", "4", "--end", "5", "--config-file", "myfile"},
			expectedErrOutput: "test error",
			appRunner:         appRunnerError,
		},
		{
			name:              "scanfill sub-command with start and end present",
			commandArgs:       []string{"scan-and-fill", "--start", "4", "--end", "5", "--config-file", "myfile"},
			expectedErrOutput: "",
			appRunner:         appRunnerSuccess,
			expectedSettings: galexie.RuntimeSettings{
				StartLedger:    4,
				EndLedger:      5,
				ConfigFilePath: "myfile",
				Mode:           galexie.ScanFill,
				Ctx:            ctx,
			},
		},
		{
			name:              "scanfill sub-command with start and end absent",
			commandArgs:       []string{"scan-and-fill", "--config-file", "myfile"},
			expectedErrOutput: "",
			appRunner:         appRunnerSuccess,
			expectedSettings: galexie.RuntimeSettings{
				StartLedger:    0,
				EndLedger:      0,
				ConfigFilePath: "myfile",
				Mode:           galexie.ScanFill,
				Ctx:            ctx,
			},
		},
		{
			name:              "scanfill sub-command prints app error",
			commandArgs:       []string{"scan-and-fill", "--start", "4", "--end", "5", "--config-file", "myfile"},
			expectedErrOutput: "test error",
			appRunner:         appRunnerError,
		},
		{
			name:              "replace sub-command with start and end present",
			commandArgs:       []string{"replace", "--start", "10", "--end", "20", "--config-file", "myfile"},
			expectedErrOutput: "",
			appRunner:         appRunnerSuccess,
			expectedSettings: galexie.RuntimeSettings{
				StartLedger:    10,
				EndLedger:      20,
				ConfigFilePath: "myfile",
				Mode:           galexie.Replace,
				Ctx:            ctx,
			},
		},
		{
			name:              "replace sub-command with start and end absent",
			commandArgs:       []string{"replace", "--config-file", "myfile"},
			expectedErrOutput: "",
			appRunner:         appRunnerSuccess,
			expectedSettings: galexie.RuntimeSettings{
				StartLedger:    0,
				EndLedger:      0,
				ConfigFilePath: "myfile",
				Mode:           galexie.Replace,
				Ctx:            ctx,
			},
		},
		{
			name:              "replace sub-command prints app error",
			commandArgs:       []string{"replace", "--start", "10", "--end", "20", "--config-file", "myfile"},
			expectedErrOutput: "test error",
			appRunner:         appRunnerError,
		},
		{
			name:              "load-test sub-command with all parameters",
			commandArgs:       []string{"load-test", "--start", "4", "--end", "5", "--merge", "--ledgers-path", "ledgers.xdr", "--close-duration", "3.5", "--config-file", "myfile"},
			expectedErrOutput: "",
			appRunner:         appRunnerSuccess,
			expectedSettings: galexie.RuntimeSettings{
				StartLedger:           4,
				EndLedger:             5,
				ConfigFilePath:        "myfile",
				Mode:                  galexie.LoadTest,
				Ctx:                   ctx,
				LoadTestMerge:         true,
				LoadTestLedgersPath:   "ledgers.xdr",
				LoadTestCloseDuration: 3500 * time.Millisecond,
			},
		},
		{
			name:              "load-test sub-command with defaults",
			commandArgs:       []string{"load-test", "--start", "4", "--end", "5", "--ledgers-path", "ledgers.xdr", "--config-file", "myfile"},
			expectedErrOutput: "",
			appRunner:         appRunnerSuccess,
			expectedSettings: galexie.RuntimeSettings{
				StartLedger:           4,
				EndLedger:             5,
				ConfigFilePath:        "myfile",
				Mode:                  galexie.LoadTest,
				Ctx:                   ctx,
				LoadTestMerge:         false,
				LoadTestLedgersPath:   "ledgers.xdr",
				LoadTestCloseDuration: 2 * time.Second,
			},
		},
		{
			name:              "load-test sub-command prints app error",
			commandArgs:       []string{"load-test", "--start", "4", "--merge", "--ledgers-path", "ledgers.xdr", "--config-file", "myfile"},
			expectedErrOutput: "test error",
			appRunner:         appRunnerError,
		},
		{
			name:              "detect-gaps sub-command with start and end present",
			commandArgs:       []string{"detect-gaps", "--start", "4", "--end", "5", "--config-file", "myfile"},
			expectedErrOutput: "",
			appRunner:         appRunnerSuccess,
			expectedSettings: galexie.RuntimeSettings{
				StartLedger:    4,
				EndLedger:      5,
				ConfigFilePath: "myfile",
				Mode:           galexie.DetectGaps,
				Ctx:            ctx,
			},
		},
		{
			name:              "detect-gaps sub-command with start and end absent",
			commandArgs:       []string{"detect-gaps", "--config-file", "myfile"},
			expectedErrOutput: "",
			appRunner:         appRunnerSuccess,
			expectedSettings: galexie.RuntimeSettings{
				StartLedger:    0,
				EndLedger:      0,
				ConfigFilePath: "myfile",
				Mode:           galexie.DetectGaps,
				Ctx:            ctx,
			},
		},
		{
			name:              "detect-gaps sub-command prints app error",
			commandArgs:       []string{"detect-gaps", "--start", "4", "--end", "5", "--config-file", "myfile"},
			expectedErrOutput: "test error",
			appRunner:         appRunnerError,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// mock galexie's cmd runner to be this test's mock routine instead of real app
			galexieCmdRunner = testCase.appRunner
			rootCmd := DefineCommands()
			rootCmd.SetArgs(testCase.commandArgs)
			var errWriter io.Writer = &bytes.Buffer{}
			var outWriter io.Writer = &bytes.Buffer{}
			rootCmd.SetErr(errWriter)
			rootCmd.SetOut(outWriter)
			rootCmd.ExecuteContext(ctx)

			errOutput := errWriter.(*bytes.Buffer).String()
			if testCase.expectedErrOutput != "" {
				assert.Contains(t, errOutput, testCase.expectedErrOutput)
			} else {
				// Ignore ReportWriter in comparison
				testResultSettings.ReportWriter = nil
				assert.Equal(t, testCase.expectedSettings, testResultSettings)
			}
		})
	}
}

func TestVersionCommand(t *testing.T) {
	rootCmd := DefineCommands()
	rootCmd.SetArgs([]string{"version"})

	var outWriter bytes.Buffer
	rootCmd.SetOut(&outWriter)

	err := rootCmd.Execute()
	require.NoError(t, err)

	output := outWriter.String()
	assert.Equal(t, "stellar-galexie "+galexie.Version()+"\n", output)
}

func TestLedgerPathCommand(t *testing.T) {
	// Test config file with proper schema settings
	testConfigFile := "../internal/test/64perfile.toml"

	testCases := []struct {
		name              string
		commandArgs       []string
		expectedOutput    string
		expectedErrOutput string
		expectError       bool
	}{
		{
			name:           "single ledger number",
			commandArgs:    []string{"ledger-path", "3811", "--config-file", testConfigFile},
			expectedOutput: "Ledger file path: FFFFF13F--3776-3839.xdr.zst\n",
			expectError:    false,
		},
		{
			name:           "single ledger with plain output",
			commandArgs:    []string{"ledger-path", "3811", "--plain", "--config-file", testConfigFile},
			expectedOutput: "FFFFF13F--3776-3839.xdr.zst\n",
			expectError:    false,
		},
		{
			name:           "ledger range",
			commandArgs:    []string{"ledger-path", "--start", "100", "--end", "200", "--config-file", testConfigFile},
			expectedOutput: "Ledger file path: FFFFFFBF--64-127.xdr.zst\nLedger file path: FFFFFF7F--128-191.xdr.zst\nLedger file path: FFFFFF3F--192-255.xdr.zst\n",
			expectError:    false,
		},
		{
			name:           "ledger range with plain output",
			commandArgs:    []string{"ledger-path", "--start", "100", "--end", "200", "--plain", "--config-file", testConfigFile},
			expectedOutput: "FFFFFFBF--64-127.xdr.zst\nFFFFFF7F--128-191.xdr.zst\nFFFFFF3F--192-255.xdr.zst\n",
			expectError:    false,
		},
		{
			name:           "start ledger only",
			commandArgs:    []string{"ledger-path", "--start", "100", "--config-file", testConfigFile},
			expectedOutput: "Ledger file path: FFFFFFBF--64-127.xdr.zst\n",
			expectError:    false,
		},
		{
			name:              "missing ledger argument",
			commandArgs:       []string{"ledger-path", "--config-file", testConfigFile},
			expectedErrOutput: "please specify a ledger number or use --start/--end flags",
			expectError:       true,
		},
		{
			name:              "invalid ledger number",
			commandArgs:       []string{"ledger-path", "abc", "--config-file", testConfigFile},
			expectedErrOutput: "invalid ledger number: abc",
			expectError:       true,
		},
		{
			name:              "invalid ledger number zero",
			commandArgs:       []string{"ledger-path", "0", "--config-file", testConfigFile},
			expectedErrOutput: "invalid ledger number: 0",
			expectError:       true,
		},
		{
			name:              "end less than start",
			commandArgs:       []string{"ledger-path", "--start", "200", "--end", "100", "--config-file", testConfigFile},
			expectedErrOutput: "end ledger (100) must be greater than or equal to start ledger (200)",
			expectError:       true,
		},
		{
			name:              "config file not found",
			commandArgs:       []string{"ledger-path", "100", "--config-file", "nonexistent.toml"},
			expectedErrOutput: "config file nonexistent.toml was not found",
			expectError:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rootCmd := DefineCommands()
			rootCmd.SetArgs(tc.commandArgs)

			var outWriter bytes.Buffer
			var errWriter bytes.Buffer
			rootCmd.SetOut(&outWriter)
			rootCmd.SetErr(&errWriter)

			err := rootCmd.Execute()

			if tc.expectError {
				require.Error(t, err)
				errOutput := errWriter.String()
				assert.Contains(t, errOutput, tc.expectedErrOutput)
			} else {
				require.NoError(t, err)
				output := outWriter.String()
				assert.Equal(t, tc.expectedOutput, output)
			}
		})
	}
}
