package pennsieve

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// Package endpoints on the legacy API host (apiHost). Used by the
// time-series flow to walk parent collections and stamp viewer-related
// properties on the target package.
//
// Mirrors processor/clients/packages_client.py.

// packageDetailResponse is the wire shape of GET /packages/{id}.
//
// Both `parent` (older API responses) and `ancestors` (newer responses)
// can contain the immediate parent's node id; we accept either. The
// Python client only reads `parent.content.nodeId` and breaks for
// root-level packages or when the API switches to the ancestors-only
// shape — handle both here.
type packageDetailResponse struct {
	Content struct {
		NodeID string `json:"nodeId"`
	} `json:"content"`
	Parent *struct {
		Content struct {
			NodeID string `json:"nodeId"`
		} `json:"content"`
	} `json:"parent,omitempty"`
	Ancestors []struct {
		Content struct {
			NodeID string `json:"nodeId"`
		} `json:"content"`
	} `json:"ancestors,omitempty"`
}

// GetParentPackageID returns the immediate parent's node id for a
// package. Returns an empty string with a non-nil error when the
// response shape carries neither `parent` nor `ancestors` — typically
// because the package is at the dataset root.
func (c *Client) GetParentPackageID(packageID string) (string, error) {
	q := url.Values{}
	q.Set("includeAncestors", "true")
	q.Set("startAtEpoch", "false")
	q.Set("limit", "100")
	q.Set("offset", "0")
	reqURL := fmt.Sprintf("%s/packages/%s?%s",
		c.apiHost, url.PathEscape(packageID), q.Encode())

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("creating get-package request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetching package %s: %w", packageID, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading package response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("fetching package %s: HTTP %d: %s", packageID, resp.StatusCode, string(body))
	}

	var detail packageDetailResponse
	if err := json.Unmarshal(body, &detail); err != nil {
		return "", fmt.Errorf("decoding package response: %w", err)
	}

	// Prefer the explicit `parent` field when present; fall back to the
	// last entry of `ancestors` (the immediate parent in ancestor lists
	// ordered root-first).
	if detail.Parent != nil && detail.Parent.Content.NodeID != "" {
		return detail.Parent.Content.NodeID, nil
	}
	if len(detail.Ancestors) > 0 {
		last := detail.Ancestors[len(detail.Ancestors)-1].Content.NodeID
		if last != "" {
			return last, nil
		}
	}
	return "", fmt.Errorf("package %s has no parent or ancestors in response (likely root-level)", packageID)
}

// PackageProperty is one entry in the properties list sent to
// PUT /packages/{id}.
type PackageProperty struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	DataType string `json:"dataType"`
	Category string `json:"category"`
	Fixed    bool   `json:"fixed"`
	Hidden   bool   `json:"hidden"`
}

type updatePropertiesRequest struct {
	Properties []PackageProperty `json:"properties"`
}

// UpdatePackageProperties writes a properties list to a package.
// Used to stamp the viewer-related metadata that lets the Pennsieve UI
// render the package as time-series.
func (c *Client) UpdatePackageProperties(packageID string, properties []PackageProperty) error {
	reqURL := fmt.Sprintf("%s/packages/%s?updateStorage=true",
		c.apiHost, url.PathEscape(packageID))
	body := updatePropertiesRequest{Properties: properties}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshaling update-properties request: %w", err)
	}

	req, err := http.NewRequest("PUT", reqURL, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("creating update-properties request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "*/*")
	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("updating properties on package %s: %w", packageID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("updating properties on package %s: HTTP %d: %s", packageID, resp.StatusCode, string(respBody))
	}
	return nil
}

// SetTimeseriesProperties stamps the viewer-related properties on a
// package so the Pennsieve UI knows to render it as a time-series
// recording. Called once per ingest on the target package.
func (c *Client) SetTimeseriesProperties(packageID string) error {
	properties := []PackageProperty{
		{
			Key:      "subtype",
			Value:    "pennsieve_timeseries",
			DataType: "string",
			Category: "Viewer",
			Fixed:    false,
			Hidden:   true,
		},
		{
			Key:      "icon",
			Value:    "timeseries",
			DataType: "string",
			Category: "Pennsieve",
			Fixed:    false,
			Hidden:   true,
		},
	}
	return c.UpdatePackageProperties(packageID, properties)
}
