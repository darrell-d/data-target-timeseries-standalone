package config

import (
	"fmt"
	"os"
)

// Config holds the workflow-runtime fields common to every data target.
// Target-specific config (asset name, properties file, etc.) lives in
// the target's own package.
type Config struct {
	InputDir       string
	APIHost        string
	APIHost2       string
	ExecutionRunID string
	CallbackToken  string
	DatasetID      string
	OrganizationID string

	// Legacy-host auth. See pennsieve.AuthConfig for resolution order.
	SessionToken    string
	PennsieveAPIKey string
	PennsieveSecret string
	CognitoRegion   string
	CognitoAppID    string
}

func Load() (*Config, error) {
	cfg := &Config{
		InputDir:        os.Getenv("INPUT_DIR"),
		APIHost:         os.Getenv("PENNSIEVE_API_HOST"),
		APIHost2:        os.Getenv("PENNSIEVE_API_HOST2"),
		ExecutionRunID:  os.Getenv("EXECUTION_RUN_ID"),
		CallbackToken:   os.Getenv("CALLBACK_TOKEN"),
		DatasetID:       os.Getenv("DATASET_ID"),
		OrganizationID:  os.Getenv("ORGANIZATION_ID"),
		SessionToken:    os.Getenv("SESSION_TOKEN"),
		PennsieveAPIKey: os.Getenv("PENNSIEVE_API_KEY"),
		PennsieveSecret: os.Getenv("PENNSIEVE_API_SECRET"),
		CognitoRegion:   os.Getenv("PENNSIEVE_COGNITO_REGION"),
		CognitoAppID:    os.Getenv("PENNSIEVE_COGNITO_APP_ID"),
	}

	if cfg.InputDir == "" {
		return nil, fmt.Errorf("INPUT_DIR is required")
	}
	if cfg.ExecutionRunID == "" {
		return nil, fmt.Errorf("EXECUTION_RUN_ID is required")
	}
	// CALLBACK_TOKEN is target-only — processors authenticate via
	// SESSION_TOKEN (Bearer) on both API hosts. Require one of the two.
	if cfg.CallbackToken == "" && cfg.SessionToken == "" {
		return nil, fmt.Errorf("either CALLBACK_TOKEN (target mode) or SESSION_TOKEN (processor mode) is required")
	}
	// DATASET_ID is target-injected too; for processor mode it's
	// derived from the execution-run lookup in the handler.
	if cfg.APIHost == "" {
		return nil, fmt.Errorf("PENNSIEVE_API_HOST is required")
	}
	if cfg.APIHost2 == "" {
		return nil, fmt.Errorf("PENNSIEVE_API_HOST2 is required")
	}
	if cfg.SessionToken == "" && cfg.PennsieveAPIKey == "" {
		return nil, fmt.Errorf("legacy-host auth not configured: set SESSION_TOKEN, or PENNSIEVE_API_KEY + PENNSIEVE_API_SECRET + PENNSIEVE_COGNITO_APP_ID")
	}
	if cfg.SessionToken == "" {
		if cfg.PennsieveSecret == "" {
			return nil, fmt.Errorf("PENNSIEVE_API_SECRET is required when PENNSIEVE_API_KEY is set")
		}
		if cfg.CognitoAppID == "" {
			return nil, fmt.Errorf("PENNSIEVE_COGNITO_APP_ID is required when PENNSIEVE_API_KEY is set")
		}
	}

	return cfg, nil
}
