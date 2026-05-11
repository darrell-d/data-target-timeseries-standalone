package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/pennsieve/data-target-timeseries-standalone/internal/shared/clients/pennsieve"
	"github.com/pennsieve/data-target-timeseries-standalone/internal/shared/config"
	"github.com/pennsieve/data-target-timeseries-standalone/internal/timeseries"
)

// LambdaEvent mirrors the per-invocation payload fields sent by the
// Step Functions Lambda invoke state.
type LambdaEvent struct {
	InputDir       string `json:"inputDir"`
	ExecutionRunID string `json:"executionRunId"`
	IntegrationID  string `json:"integrationId"`
	ComputeNodeID  string `json:"computeNodeId"`
	CallbackToken  string `json:"callbackToken"`
	DatasetID      string `json:"datasetId"`
	OrganizationID string `json:"organizationId"`
	TargetType     string `json:"targetType"`

	Params map[string]string `json:"params"`
}

// LambdaResponse is returned to Step Functions after the handler completes.
type LambdaResponse struct {
	Status         string `json:"status"`
	ExecutionRunID string `json:"executionRunId"`
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}
	auth := pennsieve.AuthConfig{
		SessionToken:  cfg.SessionToken,
		APIKey:        cfg.PennsieveAPIKey,
		APISecret:     cfg.PennsieveSecret,
		CognitoRegion: cfg.CognitoRegion,
		CognitoAppID:  cfg.CognitoAppID,
	}
	client := pennsieve.NewClient(cfg.APIHost, cfg.APIHost2, cfg.ExecutionRunID, cfg.CallbackToken, auth)
	_, err = timeseries.Run(context.Background(), cfg, client)
	return err
}

// lambdaHandler bridges the Lambda invocation payload to environment variables,
// then runs the same logic as ECS mode.
func lambdaHandler(_ context.Context, event LambdaEvent) (LambdaResponse, error) {
	slog.Info("lambda handler invoked")

	os.Setenv("INPUT_DIR", event.InputDir)
	os.Setenv("EXECUTION_RUN_ID", event.ExecutionRunID)
	os.Setenv("CALLBACK_TOKEN", event.CallbackToken)
	os.Setenv("DATASET_ID", event.DatasetID)
	os.Setenv("ORGANIZATION_ID", event.OrganizationID)
	os.Setenv("TARGET_TYPE", event.TargetType)

	for k, v := range event.Params {
		os.Setenv(k, v)
	}

	if err := run(); err != nil {
		return LambdaResponse{Status: "error", ExecutionRunID: event.ExecutionRunID}, err
	}

	return LambdaResponse{Status: "success", ExecutionRunID: event.ExecutionRunID}, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		slog.Info("detected Lambda runtime, starting RIC handler")
		lambda.Start(lambdaHandler)
	} else {
		slog.Info("running in ECS/local mode")
		if err := run(); err != nil {
			slog.Error("fatal error", "error", err)
			os.Exit(1)
		}
	}
}
