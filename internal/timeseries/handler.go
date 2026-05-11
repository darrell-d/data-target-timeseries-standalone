// Package timeseries implements the time-series data target. Its
// orchestration follows processor/importer.py's import_timeseries_via_assets
// from the processor-post-timeseries Python project.
//
// The flow:
//
//  1. Resolve the target package from the workflow's data sources.
//  2. Find or create a viewer_asset linked to all workflow packages.
//     Idempotent: a 'ready' existing asset short-circuits the run.
//  3. Create channels under the viewer_asset (FK on channels.viewer_asset_id).
//  4. Rename chunk files to use channel node ids in their basenames so
//     the streaming-side range lookup matches the legacy naming.
//  5. Upload chunks to S3 using the asset's STS credentials.
//  6. Register ranges via timeseries-service.
//  7. Mark the asset 'ready'.
//  8. On any failure: delete created channels, then delete the asset
//     (the cleanup-queue lambda purges S3 behind the asset row).
package timeseries

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"

	"github.com/pennsieve/data-target-timeseries-standalone/internal/shared/clients/pennsieve"
	"github.com/pennsieve/data-target-timeseries-standalone/internal/shared/config"
)

// File extensions produced by the upstream writer (processor-post-timeseries
// processor/writer.py). Mirrors processor/constants.py.
const (
	timeseriesBinaryExt   = ".bin.gz"
	timeseriesMetadataExt = ".metadata.json"
)

// channelIndexPattern matches the intra-processor channel ordinal
// embedded in staged filenames (e.g. "channel-00007").
var channelIndexPattern = regexp.MustCompile(`(channel-\d+)`)

// chunkTimestampPattern pulls the start/end microsecond timestamps off
// a chunk filename. Works for both pre-rename ("channel-NNNNN_...") and
// post-rename ("N:channel:..._...") basenames.
var chunkTimestampPattern = regexp.MustCompile(`_(\d+)_(\d+)\.bin\.gz$`)

// Run executes the time-series data-target flow. Returns the asset id
// on success, or an error if any step fails.
func Run(ctx context.Context, cfg *config.Config, client *pennsieve.Client) (string, error) {
	tc, err := loadTargetConfig()
	if err != nil {
		return "", err
	}

	dataFiles, channelFiles, err := collectTimeseriesFiles(cfg.InputDir)
	if err != nil {
		return "", fmt.Errorf("scanning %s: %w", cfg.InputDir, err)
	}
	if len(dataFiles) == 0 || len(channelFiles) == 0 {
		slog.Info("no time series channels or data; nothing to ingest",
			"dataFiles", len(dataFiles), "channelFiles", len(channelFiles))
		return "", nil
	}

	// Resolve dataset + package_ids from the execution run.
	execRun, err := client.GetExecutionRun(cfg.ExecutionRunID)
	if err != nil {
		return "", fmt.Errorf("fetching execution run: %w", err)
	}
	// Processor mode doesn't get DATASET_ID injected; derive it from the run.
	if cfg.DatasetID == "" {
		cfg.DatasetID = execRun.DatasetID
	}
	if cfg.DatasetID == "" {
		return "", fmt.Errorf("DATASET_ID not set and execution run has no datasetId")
	}
	packageIDs, err := pennsieve.GetPackageIDs(execRun)
	if err != nil {
		return "", err
	}

	// channelsHostPackageID is the package that channels live under and
	// that timeseries-service uses to scope range registration. It must
	// be a real N:package: node — the /timeseries/package/{id}/ranges
	// endpoint rejects collection ids with HTTP 404 since collections
	// have no channels of their own.
	//
	// We also stamp the viewer-rendering properties on this same package
	// (rather than on the parent collection) so the time-series icon and
	// the underlying channels/ranges are co-located: the user clicks
	// this package, the UI fetches its channels, the streaming server
	// has data to serve. Stamping on a parent collection puts an icon
	// on a folder whose channel lookup returns empty — misleading UX.
	channelsHostPackageID := packageIDs[0]

	if err := client.SetTimeseriesProperties(channelsHostPackageID); err != nil {
		return "", fmt.Errorf("setting timeseries properties on %s: %w", channelsHostPackageID, err)
	}
	slog.Info("updated package with time series properties",
		"packageId", channelsHostPackageID)

	slog.Info("starting asset-flow ingest",
		"datasetId", cfg.DatasetID,
		"channelsHostPackageId", channelsHostPackageID,
		"assetName", tc.AssetName,
		"assetType", tc.AssetType,
		"executionRunId", cfg.ExecutionRunID,
	)

	asset, uploadCreds, err := findOrCreateAsset(client, cfg.DatasetID, packageIDs, tc.AssetName, tc.AssetType)
	if err != nil {
		return "", fmt.Errorf("finding or creating viewer asset: %w", err)
	}

	// uploadCreds == nil signals "an already-ready asset matched; this
	// is an idempotent re-run, skip the rest." See findOrCreateAsset
	// for the status-aware semantics.
	if uploadCreds == nil {
		return asset.ID, nil
	}

	// Track only channels created by THIS run. Reused channels stay
	// untouched on cleanup since deleting them would clobber state from
	// a prior successful ingest.
	var createdChannelNodeIDs []string

	if err := runIngest(ctx, client, cfg, tc, asset, uploadCreds, channelsHostPackageID, channelFiles, dataFiles, &createdChannelNodeIDs); err != nil {
		// DEBUG: cleanup disabled so we can inspect post-failure state.
		// Channels and asset will remain in the DB. Re-enable before
		// merging / shipping for real.
		slog.Warn("DEBUG MODE: cleanup disabled — channels and asset preserved for inspection",
			"assetId", asset.ID,
			"createdChannelNodeIDs", createdChannelNodeIDs,
			"channelsHostPackageId", channelsHostPackageID)
		// runCleanup(client, cfg.DatasetID, channelsHostPackageID, asset.ID, createdChannelNodeIDs)
		return "", err
	}

	return asset.ID, nil
}

// runIngest is the post-asset, pre-ready section: channels → upload →
// ranges → mark ready. Pulled into its own function so the caller's
// error-handling can run cleanup once for any failure inside.
func runIngest(
	ctx context.Context,
	client *pennsieve.Client,
	cfg *config.Config,
	tc *targetConfig,
	asset *pennsieve.ViewerAsset,
	uploadCreds *pennsieve.UploadCredentials,
	targetPackageID string,
	channelFiles, dataFiles []string,
	createdNodeIDs *[]string,
) error {
	existingChannels, err := client.GetPackageChannels(targetPackageID)
	if err != nil {
		return fmt.Errorf("fetching existing channels: %w", err)
	}

	channelsByIndex, created, err := createOrResolveChannels(
		client, targetPackageID, channelFiles, existingChannels, asset.ID,
	)
	if err != nil {
		return err
	}
	*createdNodeIDs = created
	if len(channelsByIndex) == 0 {
		return fmt.Errorf("no channels were resolved from staged metadata files; refusing to mark asset ready with empty data")
	}

	// Rename chunk basenames from "channel-NNNNN_..." to
	// "{channel.id}_..." so streaming-side range lookups match the
	// keys we'll register and upload.
	renamed, err := renameDataFilesToNodeIDs(dataFiles, channelsByIndex)
	if err != nil {
		return err
	}
	if len(renamed) == 0 {
		return fmt.Errorf("no chunk files were resolved from the output directory; refusing to mark asset ready with empty data")
	}

	// Upload to S3 using the STS creds returned by create_asset.
	uploads, err := pennsieve.UploadChunks(ctx, uploadCreds, renamed, cfg.InputDir)
	if err != nil {
		return fmt.Errorf("uploading chunks: %w", err)
	}
	slog.Info("uploaded chunk files",
		"count", len(uploads), "assetId", asset.ID)

	// Register ranges. Build chunks from filenames + channel map.
	chunks, err := buildRangeChunks(uploads, channelsByIndex)
	if err != nil {
		return fmt.Errorf("building range chunks: %w", err)
	}
	result, err := client.CreateRangesBatched(targetPackageID, cfg.DatasetID, asset.ID, chunks)
	if err != nil {
		return fmt.Errorf("registering ranges: %w", err)
	}
	slog.Info("registered ranges",
		"assetId", asset.ID,
		"requested", result.Requested,
		"created", result.Created,
		"skipped", result.Skipped,
	)

	// status='ready' gates re-run idempotency; let failures propagate
	// so cleanup runs and the next attempt starts fresh.
	if err := client.MarkViewerAssetReady(asset.ID, cfg.DatasetID); err != nil {
		return fmt.Errorf("marking asset %s ready: %w", asset.ID, err)
	}
	return nil
}

// runCleanup deletes channels created during this run, then deletes the
// asset. Channels first: viewer_asset_id has no FK so deleting only the
// asset would orphan them and break the next run.
func runCleanup(client *pennsieve.Client, datasetID, packageID, assetID string, createdNodeIDs []string) {
	for _, nodeID := range createdNodeIDs {
		if err := client.DeleteChannel(packageID, nodeID); err != nil {
			slog.Error("failed to delete channel during cleanup",
				"channelId", nodeID, "err", err)
			continue
		}
		slog.Info("deleted channel during cleanup", "channelId", nodeID)
	}

	if err := client.DeleteAsset(assetID, datasetID); err != nil {
		slog.Error("failed to delete failed asset; needs manual cleanup",
			"assetId", assetID, "err", err)
		return
	}
	slog.Info("queued asset for cleanup", "assetId", assetID)
}

// findOrCreateAsset returns the asset to use for this ingest plus its
// upload credentials. Status-aware semantics:
//
//   - Existing asset with status='ready' → return (asset, nil).
//     uploadCreds==nil signals the caller to short-circuit; this is an
//     idempotent re-run and the asset is already done.
//   - Existing asset in any other state → assumed to be from a failed
//     prior run. Delete it (triggers S3 cleanup) and create fresh.
//   - No existing asset → create.
//
// Lookup uses workflow.package_ids (not the aggregating target package)
// because viewer_asset_packages is keyed by the per-package linkage at
// creation time; looking up by the parent collection would always miss.
func findOrCreateAsset(
	client *pennsieve.Client,
	datasetID string,
	packageIDs []string,
	assetName, assetType string,
) (*pennsieve.ViewerAsset, *pennsieve.UploadCredentials, error) {
	match, err := findAssetByWorkflowPackages(client, datasetID, packageIDs, assetName, assetType)
	if err != nil {
		return nil, nil, err
	}

	if match != nil && match.Status == "ready" {
		slog.Info("asset already ready; idempotent re-run, skipping ingest",
			"assetId", match.ID, "packageIds", packageIDs)
		return match, nil, nil
	}

	if match != nil {
		slog.Info("asset found in non-ready status; assuming prior run failed, deleting and recreating",
			"assetId", match.ID, "status", match.Status, "packageIds", packageIDs)
		if err := client.DeleteAsset(match.ID, datasetID); err != nil {
			return nil, nil, fmt.Errorf("deleting stale asset %s: %w", match.ID, err)
		}
	}

	slog.Info("creating new asset",
		"name", assetName, "type", assetType,
		"datasetId", datasetID, "packageCount", len(packageIDs),
	)
	created, err := client.CreateViewerAsset(datasetID, assetName, assetType, nil, packageIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("creating viewer asset: %w", err)
	}
	return &created.Asset, &created.UploadCredentials, nil
}

// findAssetByWorkflowPackages walks the workflow's package list and
// returns the first asset whose name+type matches. Stops on first hit;
// returns (nil, nil) when no package surfaces a match.
func findAssetByWorkflowPackages(
	client *pennsieve.Client,
	datasetID string,
	packageIDs []string,
	assetName, assetType string,
) (*pennsieve.ViewerAsset, error) {
	for _, pkgID := range packageIDs {
		assets, err := client.ListAssetsForPackage(datasetID, pkgID)
		if err != nil {
			return nil, fmt.Errorf("listing assets for package %s: %w", pkgID, err)
		}
		for i := range assets {
			a := &assets[i]
			if a.Name == assetName && a.AssetType == assetType {
				return a, nil
			}
		}
	}
	return nil, nil
}

// createOrResolveChannels walks each staged channel-metadata JSON file
// and either reuses an existing channel on the package (when name/type/
// rate match) or creates a new one with viewer_asset_id set.
//
// Returns (channelsByIndex, createdNodeIDs). The second slice holds
// only node ids the current run created — failure cleanup deletes
// these without touching reused channels.
func createOrResolveChannels(
	client *pennsieve.Client,
	packageID string,
	channelFiles []string,
	existing []*pennsieve.TimeSeriesChannel,
	viewerAssetID string,
) (map[string]*pennsieve.TimeSeriesChannel, []string, error) {
	channels := make(map[string]*pennsieve.TimeSeriesChannel, len(channelFiles))
	var createdNodeIDs []string

	for _, fp := range channelFiles {
		base := filepath.Base(fp)
		match := channelIndexPattern.FindStringSubmatch(base)
		if len(match) < 2 {
			return nil, nil, fmt.Errorf("channel metadata filename does not match expected channel-NNNNN pattern: %s", fp)
		}
		channelIndex := match[1]

		local, err := loadChannelMetadata(fp)
		if err != nil {
			return nil, nil, err
		}
		local.ViewerAssetID = viewerAssetID

		var resolved *pennsieve.TimeSeriesChannel
		for _, ec := range existing {
			if local.EqualForReuse(ec) {
				resolved = ec
				break
			}
		}

		if resolved != nil {
			if resolved.ViewerAssetID != viewerAssetID {
				return nil, nil, fmt.Errorf(
					"channel %s (%s) on package %s is linked to viewer_asset_id=%q but the current ingest expects %q. Resolve manually before re-running.",
					resolved.ID, resolved.Name, packageID, resolved.ViewerAssetID, viewerAssetID,
				)
			}
			slog.Info("reusing existing channel",
				"packageId", packageID, "channelId", resolved.ID, "name", resolved.Name)
		} else {
			created, err := client.CreateChannel(packageID, local)
			if err != nil {
				return nil, nil, fmt.Errorf("creating channel %q: %w", local.Name, err)
			}
			resolved = created
			createdNodeIDs = append(createdNodeIDs, resolved.ID)
			slog.Info("created new channel",
				"packageId", packageID, "channelId", resolved.ID, "name", resolved.Name)
		}

		resolved.Index = channelIndex
		channels[channelIndex] = resolved
	}

	return channels, createdNodeIDs, nil
}

// renameDataFilesToNodeIDs renames each chunk binary in place, swapping
// the "channel-NNNNN" prefix for the channel's node id. Mirrors the
// legacy importer's substitution so the resulting S3 keys match what
// the streaming side expects to fetch via timeseries.ranges.location.
//
// Returns the list of new paths (the original list is invalidated).
func renameDataFilesToNodeIDs(dataFiles []string, channelsByIndex map[string]*pennsieve.TimeSeriesChannel) ([]string, error) {
	renamed := make([]string, 0, len(dataFiles))
	for _, fp := range dataFiles {
		base := filepath.Base(fp)
		match := channelIndexPattern.FindStringSubmatch(base)
		if len(match) < 2 {
			return nil, fmt.Errorf("chunk filename does not match expected channel-NNNNN_*_*.bin.gz pattern: %s", fp)
		}
		channelIndex := match[1]
		ch, ok := channelsByIndex[channelIndex]
		if !ok {
			return nil, fmt.Errorf(
				"chunk file %s references channel index %q for which no channel metadata was resolved; every chunk must map to a known channel",
				fp, channelIndex,
			)
		}

		newBase := channelIndexPattern.ReplaceAllString(base, ch.ID)
		newPath := filepath.Join(filepath.Dir(fp), newBase)
		if err := os.Rename(fp, newPath); err != nil {
			return nil, fmt.Errorf("renaming %s -> %s: %w", fp, newPath, err)
		}
		renamed = append(renamed, newPath)
	}
	return renamed, nil
}

// buildRangeChunks converts each successful upload result into a
// RangeChunk for POST /ranges. Filenames are node-id-prefixed at this
// point; we pull start/end us timestamps and look up the channel by
// matching the prefix against the channel.ID.
func buildRangeChunks(
	uploads []pennsieve.ChunkUploadResult,
	channelsByIndex map[string]*pennsieve.TimeSeriesChannel,
) ([]pennsieve.RangeChunk, error) {
	channelsByNodeID := make(map[string]*pennsieve.TimeSeriesChannel, len(channelsByIndex))
	for _, ch := range channelsByIndex {
		channelsByNodeID[ch.ID] = ch
	}

	chunks := make([]pennsieve.RangeChunk, 0, len(uploads))
	for _, up := range uploads {
		base := up.RelativeKey
		idx := chunkTimestampPattern.FindStringSubmatchIndex(base)
		if idx == nil {
			return nil, fmt.Errorf("chunk filename does not contain start/end timestamps: %s", base)
		}
		// Submatch indices: [fullStart, fullEnd, group1Start, group1End, group2Start, group2End]
		start, err := parseInt64(base[idx[2]:idx[3]])
		if err != nil {
			return nil, fmt.Errorf("parsing start timestamp from %s: %w", base, err)
		}
		end, err := parseInt64(base[idx[4]:idx[5]])
		if err != nil {
			return nil, fmt.Errorf("parsing end timestamp from %s: %w", base, err)
		}
		nodeID := base[:idx[0]]
		ch, ok := channelsByNodeID[nodeID]
		if !ok {
			return nil, fmt.Errorf("chunk basename %s has no matching channel (node_id=%s)", base, nodeID)
		}
		// Upstream chunker writes filenames with INCLUSIVE end timestamps
		// (last sample in chunk). timeseries-service stores ranges as
		// [start, end) and rejects start >= end, so single-sample chunks
		// (start == end in the filename) need a 1us bump to be valid.
		if end <= start {
			end = start + 1
		}
		chunks = append(chunks, pennsieve.RangeChunk{
			ChannelNodeID: ch.ID,
			Start:         start,
			End:           end,
			S3Key:         base,
		})
	}
	return chunks, nil
}

// determineTargetPackage chooses which package should receive the
// time-series viewer properties.
//
//   - Single package_id → that package directly.
//   - Multiple package_ids → walk to the parent collection of the
//     first "N:package:..."-prefixed entry.
//
// Returns "" with a nil error when no package id is workable
// (caller logs and aborts the ingest cleanly).
func determineTargetPackage(client *pennsieve.Client, packageIDs []string) (string, error) {
	if len(packageIDs) == 0 {
		slog.Warn("no package IDs provided")
		return "", nil
	}
	if len(packageIDs) == 1 {
		slog.Info("single package id; using directly", "packageId", packageIDs[0])
		return packageIDs[0], nil
	}

	var firstPackage string
	for _, pid := range packageIDs {
		if len(pid) >= 10 && pid[:10] == "N:package:" {
			firstPackage = pid
			break
		}
	}
	if firstPackage == "" {
		slog.Warn("no N:package:-prefixed id in package list", "packageIds", packageIDs)
		return "", nil
	}

	slog.Info("multiple package ids; walking to parent of first package",
		"firstPackage", firstPackage)
	parentID, err := client.GetParentPackageID(firstPackage)
	if err != nil {
		// Common case: the source packages are flat at the dataset root,
		// so there's no enclosing collection to use as the target. Fall
		// back to the first package id rather than aborting the run; the
		// viewer-properties stamp and range registration just land on
		// that package instead of an aggregating parent.
		slog.Warn("parent walk failed; falling back to first package as target",
			"firstPackage", firstPackage, "err", err)
		return firstPackage, nil
	}
	slog.Info("resolved parent package id", "parentId", parentID)
	return parentID, nil
}

// collectTimeseriesFiles walks fileDir and partitions files by
// extension into (data, channel) lists. Mirrors
// processor/importer.py _collect_timeseries_files.
func collectTimeseriesFiles(fileDir string) (dataFiles, channelFiles []string, err error) {
	err = filepath.Walk(fileDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		base := info.Name()
		switch {
		case hasSuffix(base, timeseriesMetadataExt):
			channelFiles = append(channelFiles, path)
		case hasSuffix(base, timeseriesBinaryExt):
			dataFiles = append(dataFiles, path)
		}
		return nil
	})
	return
}

// loadChannelMetadata reads a single channel JSON file written by the
// upstream processor. The file content is the channel's serialized
// metadata in the same shape as the legacy timeseries API request body.
func loadChannelMetadata(path string) (*pennsieve.TimeSeriesChannel, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening channel metadata %s: %w", path, err)
	}
	defer f.Close()

	// The on-disk format mirrors the API create-request body keys
	// (writer.py uses TimeSeriesChannel.as_dict for serialization).
	var raw struct {
		ID             string           `json:"id,omitempty"`
		ViewerAssetID  string           `json:"viewerAssetId,omitempty"`
		Name           string           `json:"name"`
		Rate           float64          `json:"rate"`
		Start          int64            `json:"start"`
		End            int64            `json:"end"`
		Type           string           `json:"type,omitempty"`
		ChannelType    string           `json:"channelType,omitempty"`
		Group          string           `json:"group"`
		LastAnnotation int64            `json:"lastAnnotation"`
		Properties     []map[string]any `json:"properties"`
	}
	if err := json.NewDecoder(f).Decode(&raw); err != nil {
		return nil, fmt.Errorf("parsing channel metadata %s: %w", path, err)
	}
	typ := raw.ChannelType
	if typ == "" {
		typ = raw.Type
	}
	return &pennsieve.TimeSeriesChannel{
		ID:             raw.ID,
		ViewerAssetID:  raw.ViewerAssetID,
		Name:           raw.Name,
		Rate:           raw.Rate,
		Start:          raw.Start,
		End:            raw.End,
		Type:           typ,
		Group:          raw.Group,
		LastAnnotation: raw.LastAnnotation,
		Properties:     raw.Properties,
	}, nil
}

func parseInt64(s string) (int64, error) {
	var n int64
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("non-digit %q in numeric field", r)
		}
		n = n*10 + int64(r-'0')
	}
	return n, nil
}

func hasSuffix(s, suffix string) bool {
	if len(s) < len(suffix) {
		return false
	}
	return s[len(s)-len(suffix):] == suffix
}
