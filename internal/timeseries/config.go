package timeseries

import (
	"fmt"
	"os"
)

// targetConfig holds the time-series-target-specific env vars. Loaded
// from environment in addition to the shared config.Config.
type targetConfig struct {
	AssetName string
	AssetType string
}

// loadTargetConfig reads the time-series-target env vars set by the
// workflow orchestrator. ASSET_NAME is required (set per-deployment to
// label the viewer asset, e.g. "mef-asset" or "edf-asset"). ASSET_TYPE
// defaults to "timeseries" since this binary only produces time-series
// viewer assets.
func loadTargetConfig() (*targetConfig, error) {
	tc := &targetConfig{
		AssetName: os.Getenv("ASSET_NAME"),
		AssetType: os.Getenv("ASSET_TYPE"),
	}

	if tc.AssetName == "" {
		return nil, fmt.Errorf("ASSET_NAME is required")
	}
	if tc.AssetType == "" {
		tc.AssetType = "timeseries"
	}

	return tc, nil
}
