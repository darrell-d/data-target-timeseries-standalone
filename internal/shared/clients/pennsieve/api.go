package pennsieve

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const bearerRefreshSkew = 5 * time.Minute

// AuthConfig configures how Client authenticates against the legacy
// Pennsieve API host. The api2 host always uses the workflow callback
// scheme (executionRunID + callbackToken) and is unaffected by this.
//
// Resolution order in setAuthHeader for the legacy host:
//   1. SessionToken set → used directly as the Bearer (no mint, no cache).
//      This is the path used when running as a processor node, where the
//      orchestrator natively injects SESSION_TOKEN.
//   2. APIKey + APISecret + CognitoAppID set → minted via Cognito
//      USER_PASSWORD_AUTH and cached until near expiry.
//   3. Otherwise → falls back to the Callback scheme (legacy host will
//      most likely reject it, but the resulting 401 surfaces cleanly).
type AuthConfig struct {
	SessionToken  string
	APIKey        string
	APISecret     string
	CognitoRegion string
	CognitoAppID  string
}

// bearerCache holds a minted Cognito access token and its expiry.
// Refreshed lazily by Client.getBearer when within bearerRefreshSkew of
// expiry. Unused when AuthConfig.SessionToken is set.
type bearerCache struct {
	mu     sync.Mutex
	token  string
	expiry time.Time
}

// Client is a minimal HTTP client for the Pennsieve API endpoints needed
// by data target binaries.
//
// Pennsieve has two distinct API hosts:
//
//   - apiHost (e.g. https://api.pennsieve.io): the legacy API. Hosts the
//     time-series channels endpoints and the package-properties endpoints.
//   - apiHost2 (e.g. https://api2.pennsieve.io): the newer API gateway.
//     Hosts viewer-asset endpoints and the timeseries-service ranges
//     endpoint.
//
// Both share the same callback-style auth header.
type Client struct {
	apiHost        string
	apiHost2       string
	executionRunID string
	callbackToken  string
	auth           AuthConfig
	httpClient     *http.Client
	bearer         bearerCache
}

func NewClient(apiHost, apiHost2, executionRunID, callbackToken string, auth AuthConfig) *Client {
	if auth.CognitoRegion == "" {
		auth.CognitoRegion = "us-east-1"
	}
	return &Client{
		apiHost:        apiHost,
		apiHost2:       apiHost2,
		executionRunID: executionRunID,
		callbackToken:  callbackToken,
		auth:           auth,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ExecutionRunDetail holds the fields returned by GET /workflows/runs/{runId}.
type ExecutionRunDetail struct {
	Uuid        string                     `json:"uuid"`
	DatasetID   string                     `json:"datasetId"`
	DataSources map[string]DataSourceInput `json:"dataSources,omitempty"`
}

// DataSourceInput holds the per-data-source inputs for a workflow execution run.
type DataSourceInput struct {
	PackageIDs []string `json:"packageIds"`
	Path       string   `json:"path,omitempty"`
}

// UploadCredentials holds temporary AWS credentials for S3 uploads.
type UploadCredentials struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token"`
	Expiration      string `json:"expiration"`
	Bucket          string `json:"bucket"`
	Region          string `json:"region"`
	KeyPrefix       string `json:"key_prefix"`
}

// ViewerAsset is the response shape returned by asset CRUD endpoints.
type ViewerAsset struct {
	ID         string   `json:"id"`
	DatasetID  string   `json:"dataset_id"`
	Name       string   `json:"name"`
	AssetType  string   `json:"asset_type"`
	Status     string   `json:"status"`
	PackageIDs []string `json:"package_ids,omitempty"`
}

// listAssetsResponse is the wire shape of GET /packages/assets.
type listAssetsResponse struct {
	Assets []ViewerAsset `json:"assets"`
}

// CreateAssetResult is returned from CreateViewerAsset.
type CreateAssetResult struct {
	Asset             ViewerAsset       `json:"asset"`
	UploadCredentials UploadCredentials `json:"upload_credentials"`
}

type createViewerAssetRequest struct {
	Name       string                 `json:"name"`
	AssetType  string                 `json:"asset_type"`
	Properties map[string]interface{} `json:"properties"`
	PackageIDs []string               `json:"package_ids,omitempty"`
}

type updateViewerAssetRequest struct {
	Status *string `json:"status,omitempty"`
}

// GetExecutionRun fetches the execution run to resolve data sources and package IDs.
func (c *Client) GetExecutionRun(runID string) (*ExecutionRunDetail, error) {
	reqURL := fmt.Sprintf("%s/compute/workflows/runs/%s", c.apiHost2, url.PathEscape(runID))

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating execution run request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	c.setAuthHeader(req)

	var run ExecutionRunDetail
	if err := c.doJSON(req, &run); err != nil {
		return nil, fmt.Errorf("fetching execution run: %w", err)
	}
	return &run, nil
}

// GetPackageIDs extracts all package IDs from the execution run's data sources.
func GetPackageIDs(run *ExecutionRunDetail) ([]string, error) {
	if len(run.DataSources) == 0 {
		return nil, fmt.Errorf("execution run has no data sources")
	}
	var packageIDs []string
	for _, ds := range run.DataSources {
		packageIDs = append(packageIDs, ds.PackageIDs...)
	}
	if len(packageIDs) == 0 {
		return nil, fmt.Errorf("execution run has no package IDs")
	}
	return packageIDs, nil
}

// CreateViewerAsset creates a viewer asset and returns upload credentials.
func (c *Client) CreateViewerAsset(datasetID, name, assetType string, properties map[string]interface{}, packageIDs []string) (*CreateAssetResult, error) {
	reqURL := fmt.Sprintf("%s/packages/assets?dataset_id=%s", c.apiHost2, url.QueryEscape(datasetID))

	body := createViewerAssetRequest{
		Name:       name,
		AssetType:  assetType,
		Properties: properties,
		PackageIDs: packageIDs,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling create asset request: %w", err)
	}

	req, err := http.NewRequest("POST", reqURL, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("creating asset request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuthHeader(req)

	var result CreateAssetResult
	if err := c.doJSON(req, &result); err != nil {
		return nil, fmt.Errorf("creating viewer asset: %w", err)
	}
	return &result, nil
}

// MarkViewerAssetReady marks a viewer asset as ready after upload completes.
func (c *Client) MarkViewerAssetReady(assetID, datasetID string) error {
	reqURL := fmt.Sprintf("%s/packages/assets/%s?dataset_id=%s",
		c.apiHost2,
		url.PathEscape(assetID),
		url.QueryEscape(datasetID),
	)

	status := "ready"
	body := updateViewerAssetRequest{Status: &status}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshaling update request: %w", err)
	}

	req, err := http.NewRequest("PATCH", reqURL, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("creating update request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuthHeader(req)

	var result json.RawMessage
	if err := c.doJSON(req, &result); err != nil {
		return fmt.Errorf("marking asset ready: %w", err)
	}
	return nil
}

// ListAssetsForPackage returns all viewer assets attached to a package.
// Used by the time-series flow to look up an existing asset for
// idempotent re-runs.
func (c *Client) ListAssetsForPackage(datasetID, packageID string) ([]ViewerAsset, error) {
	q := url.Values{}
	q.Set("dataset_id", datasetID)
	q.Set("package_id", packageID)
	reqURL := fmt.Sprintf("%s/packages/assets?%s", c.apiHost2, q.Encode())

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating list-assets request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	c.setAuthHeader(req)

	var result listAssetsResponse
	if err := c.doJSON(req, &result); err != nil {
		return nil, fmt.Errorf("listing viewer assets for package %s: %w", packageID, err)
	}
	return result.Assets, nil
}

// DeleteAsset deletes a viewer asset. Triggers async S3 cleanup via the
// cleanup-queue lambda; safe to call from failure-path cleanup.
func (c *Client) DeleteAsset(assetID, datasetID string) error {
	reqURL := fmt.Sprintf("%s/packages/assets/%s?dataset_id=%s",
		c.apiHost2,
		url.PathEscape(assetID),
		url.QueryEscape(datasetID),
	)

	req, err := http.NewRequest("DELETE", reqURL, nil)
	if err != nil {
		return fmt.Errorf("creating delete-asset request: %w", err)
	}
	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("deleting viewer asset %s: %w", assetID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("deleting viewer asset %s: HTTP %d: %s", assetID, resp.StatusCode, string(body))
	}
	return nil
}

func (c *Client) setAuthHeader(req *http.Request) {
	// Processor mode: SESSION_TOKEN is a Cognito Bearer that works against
	// both API hosts. Use it unconditionally when present.
	if c.auth.SessionToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.auth.SessionToken)
		return
	}
	// Target mode: legacy host requires a Bearer (Callback is api2-only).
	// Mint one via Cognito if API key + secret + app id are configured.
	if legacyURL, err := url.Parse(c.apiHost); err == nil && req.URL.Host == legacyURL.Host {
		if c.auth.APIKey != "" && c.auth.APISecret != "" && c.auth.CognitoAppID != "" {
			if tok, err := c.getBearer(); err == nil {
				req.Header.Set("Authorization", "Bearer "+tok)
				return
			}
			// Fall through to Callback on mint failure; the request will
			// likely 401 on the legacy host but the error surfaces cleanly.
		}
	}
	req.Header.Set("Authorization",
		fmt.Sprintf("Callback workflow-service:%s:%s", c.executionRunID, c.callbackToken))
}

// getBearer returns a cached Cognito access token, minting a fresh one
// when the cached value is missing or within bearerRefreshSkew of
// expiry. Safe for concurrent use. Only called when AuthConfig.APIKey
// is set (the mint path); the SessionToken path skips this entirely.
func (c *Client) getBearer() (string, error) {
	c.bearer.mu.Lock()
	defer c.bearer.mu.Unlock()

	if c.bearer.token != "" && time.Now().Before(c.bearer.expiry.Add(-bearerRefreshSkew)) {
		return c.bearer.token, nil
	}

	tok, expiresIn, err := mintCognitoAccessToken(c.httpClient, c.auth.CognitoRegion, c.auth.CognitoAppID, c.auth.APIKey, c.auth.APISecret)
	if err != nil {
		return "", fmt.Errorf("minting Cognito access token: %w", err)
	}
	c.bearer.token = tok
	c.bearer.expiry = time.Now().Add(time.Duration(expiresIn) * time.Second)
	return tok, nil
}

// mintCognitoAccessToken performs Cognito's USER_PASSWORD_AUTH InitiateAuth
// against the given app client and returns (accessToken, expiresInSeconds).
// Cognito InitiateAuth is unauthenticated (requires no AWS creds), so we
// post directly to the cognito-idp endpoint and avoid pulling in the
// AWS SDK service module.
func mintCognitoAccessToken(httpClient *http.Client, region, clientID, username, password string) (string, int, error) {
	endpoint := fmt.Sprintf("https://cognito-idp.%s.amazonaws.com/", region)
	body := map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": username,
			"PASSWORD": password,
		},
		"ClientId": clientID,
	}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return "", 0, fmt.Errorf("marshaling InitiateAuth body: %w", err)
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(jsonBody))
	if err != nil {
		return "", 0, fmt.Errorf("building InitiateAuth request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService.InitiateAuth")

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("InitiateAuth request failed: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("reading InitiateAuth response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", 0, fmt.Errorf("InitiateAuth: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var parsed struct {
		AuthenticationResult struct {
			AccessToken string `json:"AccessToken"`
			ExpiresIn   int    `json:"ExpiresIn"`
		} `json:"AuthenticationResult"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", 0, fmt.Errorf("decoding InitiateAuth response: %w", err)
	}
	if parsed.AuthenticationResult.AccessToken == "" {
		return "", 0, fmt.Errorf("InitiateAuth response missing AccessToken: %s", string(respBody))
	}
	return parsed.AuthenticationResult.AccessToken, parsed.AuthenticationResult.ExpiresIn, nil
}

func (c *Client) doJSON(req *http.Request, result interface{}) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	return nil
}
