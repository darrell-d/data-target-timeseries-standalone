// Command orphaned-channels lists — and optionally deletes — time-series
// channels left behind on a package by a failed ingest.
//
// Why this exists: when an ingest fails, runCleanup deletes the channels it
// created. If that rollback is skipped, the channels outlive their asset. A
// later run deletes the stale asset and creates a fresh one, and
// createOrResolveChannels then refuses to adopt the survivors:
//
//	channel N:channel:... (EKG1) on package N:package:... is linked to
//	viewer_asset_id="" but the current ingest expects "..."
//
// Nothing clears that state automatically — runCleanup only ever deletes
// channels created by the run that is failing, never pre-existing ones.
// Hence a manual tool.
//
// # How orphaned-ness is determined
//
// NOT by the channel's viewer_asset_id. GET /timeseries/{pkg}/channels does
// not return that field at all: its response carries only channelType,
// createdAt, end, group, id, lastAnnotation, name, packageId, rate, start
// and unit. So TimeSeriesChannel.ViewerAssetID unmarshals to "" for every
// channel, whether it is linked or not, and "absent field" is
// indistinguishable from "owned by nobody". A tool keying off that value
// would label every channel on every package as orphaned — including
// channels belonging to a healthy, ready asset.
//
// Instead this asks the question from the asset side: a channel can only be
// owned by an asset attached to its package, so if the package has no assets
// at all, every channel on it is provably an orphan. That is the only case
// this tool will delete in without an explicit override.
//
// Usage — list only (default, touches nothing):
//
//	PACKAGE_ID=N:package:... DATASET_ID=N:dataset:... \
//	  PENNSIEVE_API_HOST=... PENNSIEVE_API_HOST2=... \
//	  PENNSIEVE_API_KEY=... PENNSIEVE_API_SECRET=... \
//	  go run ./cmd/orphaned-channels
//
// Delete, after reviewing the listing:
//
//	... CONFIRM_DELETE=yes go run ./cmd/orphaned-channels
//
// When assets are still attached, deleting channels is refused: any of them
// could be the rightful owner, including an in-progress run's. Delete the
// stale asset first (ASSET_IDS=...), then re-run — with no assets left, the
// channels are provably orphaned. A ready asset is never overridable.
package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/pennsieve/data-target-timeseries-standalone/internal/shared/clients/pennsieve"
)

// assetStatusReady is the terminal, in-use state. Channels under a ready
// asset are live data and must never be deleted by this tool.
const assetStatusReady = "ready"

func main() {
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Fatalf("error: %v", err)
	}
}

func run() error {
	packageID := os.Getenv("PACKAGE_ID")
	datasetID := os.Getenv("DATASET_ID")
	apiHost := os.Getenv("PENNSIEVE_API_HOST")
	apiHost2 := os.Getenv("PENNSIEVE_API_HOST2")

	// DATASET_ID is required now, not optional: the asset lookup that
	// establishes orphaned-ness is dataset-scoped.
	if packageID == "" || datasetID == "" || apiHost == "" || apiHost2 == "" {
		return fmt.Errorf("PACKAGE_ID, DATASET_ID, PENNSIEVE_API_HOST and PENNSIEVE_API_HOST2 are required")
	}

	client := pennsieve.NewClient(apiHost, apiHost2, "", os.Getenv("CALLBACK_TOKEN"),
		pennsieve.AuthConfig{
			SessionToken:  os.Getenv("SESSION_TOKEN"),
			RefreshToken:  os.Getenv("REFRESH_TOKEN"),
			APIKey:        os.Getenv("PENNSIEVE_API_KEY"),
			APISecret:     os.Getenv("PENNSIEVE_API_SECRET"),
			CognitoRegion: os.Getenv("PENNSIEVE_COGNITO_REGION"),
			CognitoAppID:  os.Getenv("PENNSIEVE_COGNITO_APP_ID"),
		})

	assets, err := client.ListAssetsForPackage(datasetID, packageID)
	if err != nil {
		return fmt.Errorf("listing assets on %s: %w", packageID, err)
	}
	channels, err := client.GetPackageChannels(packageID)
	if err != nil {
		return fmt.Errorf("listing channels on %s: %w", packageID, err)
	}

	fmt.Printf("package %s\n", packageID)
	fmt.Printf("  channels: %d\n", len(channels))
	fmt.Printf("  assets:   %d\n", len(assets))
	var ready []pennsieve.ViewerAsset
	for _, a := range assets {
		fmt.Printf("      %s  name=%q type=%s status=%s\n", a.ID, a.Name, a.AssetType, a.Status)
		if a.Status == assetStatusReady {
			ready = append(ready, a)
		}
	}
	fmt.Println()

	if len(channels) == 0 {
		fmt.Println("No channels on this package. Nothing to do.")
		return nil
	}

	// Deleting assets is explicit and happens first: clearing the stale
	// asset is what makes the remaining channels provably orphaned.
	assetIDs := splitNonEmpty(os.Getenv("ASSET_IDS"))
	confirmed := os.Getenv("CONFIRM_DELETE") == "yes"

	if len(assetIDs) > 0 {
		for _, id := range assetIDs {
			if isReady(assets, id) {
				return fmt.Errorf("refusing to delete asset %s: status is %q, so its channels are live data", id, assetStatusReady)
			}
		}
		if !confirmed {
			fmt.Printf("DRY RUN: would delete %d asset(s): %s\n", len(assetIDs), strings.Join(assetIDs, ", "))
		} else {
			for _, id := range assetIDs {
				if err := client.DeleteAsset(id, datasetID); err != nil {
					return fmt.Errorf("deleting asset %s: %w", id, err)
				}
				fmt.Printf("deleted asset %s\n", id)
			}
			// Re-read so the safety check below sees the new state rather
			// than the snapshot taken before these deletions.
			assets, err = client.ListAssetsForPackage(datasetID, packageID)
			if err != nil {
				return fmt.Errorf("re-listing assets after deletion: %w", err)
			}
			ready = nil
			for _, a := range assets {
				if a.Status == assetStatusReady {
					ready = append(ready, a)
				}
			}
		}
	}

	// The safety gate. Channels are provably orphaned only when nothing on
	// the package could own them.
	switch {
	case len(ready) > 0:
		return fmt.Errorf(
			"refusing to delete %d channels: package has %d asset(s) in status %q, whose channels are live data. "+
				"There is no per-channel ownership field in the API to tell them apart, so deleting here risks destroying a working asset",
			len(channels), len(ready), assetStatusReady)
	case len(assets) > 0:
		if os.Getenv("DELETE_WITH_ASSETS_PRESENT") != "yes" {
			fmt.Printf("REFUSING: %d channels present, but %d asset(s) are still attached to this package.\n", len(channels), len(assets))
			fmt.Println("Any of them could own these channels — including a run that is in progress right now.")
			fmt.Println("Delete the stale asset first (ASSET_IDS=<id> CONFIRM_DELETE=yes), then re-run;")
			fmt.Println("with no assets left the channels are provably orphaned.")
			fmt.Println("To override anyway (you have confirmed no run is active), set DELETE_WITH_ASSETS_PRESENT=yes.")
			return nil
		}
		fmt.Printf("WARNING: deleting channels while %d non-ready asset(s) remain, per DELETE_WITH_ASSETS_PRESENT=yes\n", len(assets))
	default:
		fmt.Printf("No assets on this package, so all %d channels are orphaned.\n", len(channels))
	}

	if !confirmed {
		fmt.Printf("DRY RUN: %d channels would be deleted. Re-run with CONFIRM_DELETE=yes.\n", len(channels))
		for i, ch := range channels {
			if i == 5 {
				fmt.Printf("      … and %d more\n", len(channels)-5)
				break
			}
			fmt.Printf("      %s  %s\n", ch.ID, ch.Name)
		}
		return nil
	}

	fmt.Printf("Deleting %d channels from %s\n", len(channels), packageID)
	var failed int
	for _, ch := range channels {
		if err := client.DeleteChannel(packageID, ch.ID); err != nil {
			// Keep going: one failure should not strand the rest, and the
			// run is repeatable — deleted channels simply do not come back
			// in the next listing.
			log.Printf("  FAILED %s (%s): %v", ch.ID, ch.Name, err)
			failed++
			continue
		}
		fmt.Printf("  deleted %s (%s)\n", ch.ID, ch.Name)
	}
	if failed > 0 {
		return fmt.Errorf("%d deletions failed; re-run to retry the remainder", failed)
	}
	fmt.Println("Done. Re-run the workflow.")
	return nil
}

func isReady(assets []pennsieve.ViewerAsset, id string) bool {
	for _, a := range assets {
		if a.ID == id {
			return a.Status == assetStatusReady
		}
	}
	return false
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
