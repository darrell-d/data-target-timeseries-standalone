package pennsieve

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// Channel CRUD against the legacy Pennsieve timeseries API.
//
// The legacy host (config.APIHost, e.g. https://api.pennsieve.io) is
// distinct from APIHost2 used by the asset endpoints — channels live on
// the older API path. Mirrors processor/clients/timeseries_client.py.

// GetPackageChannels returns all channels attached to a time-series
// package. Used during ingest to find existing channels we can reuse.
func (c *Client) GetPackageChannels(packageID string) ([]*TimeSeriesChannel, error) {
	reqURL := fmt.Sprintf("%s/timeseries/%s/channels", c.apiHost, url.PathEscape(packageID))

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating get-channels request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching channels for package %s: %w", packageID, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading get-channels response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("fetching channels for package %s: HTTP %d: %s", packageID, resp.StatusCode, string(body))
	}

	var wire []channelResponse
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, fmt.Errorf("decoding channels response: %w", err)
	}
	out := make([]*TimeSeriesChannel, len(wire))
	for i := range wire {
		out[i] = wire[i].toChannel()
	}
	return out, nil
}

// CreateChannel creates a single channel on the package. The channel's
// ViewerAssetID must be set so the row gets linked to the viewer asset
// (the FK enforces this on the timeseries-service side).
//
// Returns the server-side channel record (with its assigned node id).
// The returned channel preserves the input's Index for caller-side
// lookup convenience.
func (c *Client) CreateChannel(packageID string, channel *TimeSeriesChannel) (*TimeSeriesChannel, error) {
	reqURL := fmt.Sprintf("%s/timeseries/%s/channels", c.apiHost, url.PathEscape(packageID))

	jsonBody, err := json.Marshal(channel.CreateRequestBody())
	if err != nil {
		return nil, fmt.Errorf("marshaling create-channel request: %w", err)
	}

	req, err := http.NewRequest("POST", reqURL, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("creating create-channel request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("creating channel %q on package %s: %w", channel.Name, packageID, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading create-channel response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("creating channel %q on package %s: HTTP %d: %s", channel.Name, packageID, resp.StatusCode, string(body))
	}

	var wire channelResponse
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, fmt.Errorf("decoding create-channel response: %w", err)
	}
	created := wire.toChannel()
	created.Index = channel.Index
	return created, nil
}

// DeleteChannel removes a channel by node id. Best-effort cleanup path:
// called by the ingest orchestrator when a run fails after channels were
// created, so the next run can start with a clean package state.
func (c *Client) DeleteChannel(packageID, channelID string) error {
	reqURL := fmt.Sprintf("%s/timeseries/%s/channels/%s",
		c.apiHost, url.PathEscape(packageID), url.PathEscape(channelID))

	req, err := http.NewRequest("DELETE", reqURL, nil)
	if err != nil {
		return fmt.Errorf("creating delete-channel request: %w", err)
	}
	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("deleting channel %s on package %s: %w", channelID, packageID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("deleting channel %s on package %s: HTTP %d: %s", channelID, packageID, resp.StatusCode, string(body))
	}
	return nil
}
