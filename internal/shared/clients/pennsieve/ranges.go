package pennsieve

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// Time-series range registration against timeseries-service (api2 host).
// Mirrors processor/clients/timeseries_ranges_client.py.

// MaxChunksPerRangeRequest mirrors dto.MaxChunksPerCreateRangeRequest in
// timeseries-service. Larger batches are rejected at the API gateway.
const MaxChunksPerRangeRequest = 10_000

// RangeChunk describes one time-series chunk to register with
// timeseries-service.
//
// S3Key is RELATIVE to the asset's prefix — no leading slash, no '..',
// no 's3://...'. timeseries-service prepends the prefix server-side.
type RangeChunk struct {
	ChannelNodeID string `json:"channel_node_id"`
	Start         int64  `json:"start"`
	End           int64  `json:"end"`
	S3Key         string `json:"s3_key"`
}

// CreateRangesResult is the aggregated outcome of one or more
// POST /ranges calls. `requested` is the count of chunks sent;
// `created` is the count newly inserted; `skipped` is the count
// already present (idempotent re-registration).
type CreateRangesResult struct {
	Requested int `json:"requested"`
	Created   int `json:"created"`
	Skipped   int `json:"skipped"`
}

type createRangesRequest struct {
	ViewerAssetID string       `json:"viewer_asset_id"`
	Chunks        []RangeChunk `json:"chunks"`
}

// CreateRanges registers a single batch of range chunks. Caller must
// ensure len(chunks) <= MaxChunksPerRangeRequest; use
// CreateRangesBatched for arbitrary-sized inputs.
func (c *Client) CreateRanges(packageNodeID, datasetNodeID, viewerAssetID string, chunks []RangeChunk) (*CreateRangesResult, error) {
	if len(chunks) == 0 {
		return &CreateRangesResult{}, nil
	}
	if len(chunks) > MaxChunksPerRangeRequest {
		return nil, fmt.Errorf("too many chunks (%d); max %d", len(chunks), MaxChunksPerRangeRequest)
	}
	if datasetNodeID == "" {
		return nil, fmt.Errorf("datasetNodeID is required for create-ranges")
	}

	q := url.Values{}
	q.Set("dataset_id", datasetNodeID)
	reqURL := fmt.Sprintf("%s/timeseries/package/%s/ranges?%s",
		c.apiHost2, url.PathEscape(packageNodeID), q.Encode())
	body := createRangesRequest{
		ViewerAssetID: viewerAssetID,
		Chunks:        chunks,
	}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling create-ranges request: %w", err)
	}

	req, err := http.NewRequest("POST", reqURL, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("creating create-ranges request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("registering ranges for package %s asset %s: %w", packageNodeID, viewerAssetID, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading create-ranges response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// 400 with InvalidChunks payload is the most useful failure
		// detail — surface the body verbatim for debugging.
		return nil, fmt.Errorf("registering ranges for package %s asset %s: HTTP %d: %s",
			packageNodeID, viewerAssetID, resp.StatusCode, string(respBody))
	}

	var result CreateRangesResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decoding create-ranges response: %w", err)
	}
	return &result, nil
}

// CreateRangesBatched splits chunks into MaxChunksPerRangeRequest-sized
// batches and POSTs each. Returns aggregated counts.
//
// Stops on the first failure — earlier batches' ranges remain inserted
// (no rollback). The caller's failure path should delete the asset to
// invalidate any partial state.
func (c *Client) CreateRangesBatched(packageNodeID, datasetNodeID, viewerAssetID string, chunks []RangeChunk) (*CreateRangesResult, error) {
	total := &CreateRangesResult{}
	for i := 0; i < len(chunks); i += MaxChunksPerRangeRequest {
		end := i + MaxChunksPerRangeRequest
		if end > len(chunks) {
			end = len(chunks)
		}
		batch := chunks[i:end]
		result, err := c.CreateRanges(packageNodeID, datasetNodeID, viewerAssetID, batch)
		if err != nil {
			return nil, err
		}
		total.Requested += result.Requested
		total.Created += result.Created
		total.Skipped += result.Skipped
	}
	return total, nil
}
