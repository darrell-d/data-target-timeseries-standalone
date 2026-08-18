package pennsieve

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const bearerRefreshSkew = 5 * time.Minute

// cognitoEndpointOverride points InitiateAuth at a stub server. Set by tests
// only; empty in every real run.
var cognitoEndpointOverride string

// AuthConfig configures how Client obtains a Bearer token.
//
// Resolution order in getBearer:
//  1. SessionToken, while it still has more than bearerRefreshSkew left.
//     This is what the orchestrator injects for a processor node.
//  2. RefreshToken exchanged via Cognito REFRESH_TOKEN_AUTH.
//  3. APIKey + APISecret minted via Cognito USER_PASSWORD_AUTH. CognitoAppID
//     is optional — discovered from the cognito-config endpoint when unset.
//
// Resolution order is (3), (1), (2): key/secret is preferred whenever it is
// configured, because it is the only source that survives a long run.
//
// Every result is cached until near expiry and re-derived on demand, so a
// stage that runs for hours keeps signing requests with a live token.
//
// Why this matters: SESSION_TOKEN is minted once when the workflow run
// starts and stored in Secrets Manager; every stage reads that same value
// and nothing refreshes it. A Cognito access token lives ~60 minutes. Any
// chain longer than that hands its later stages a dead token, and the
// api2 authorizer rejects it with a bare 403 Forbidden.
//
// The refresh token is not a way out either. Its lifetime is anchored to the
// human login that produced it (auth_time), not to this run, and refreshing
// yields a new access token but never a new refresh token — so the clock
// never resets. A workflow launched from a day-old browser session can begin
// with both tokens already dead. An API key/secret pair has no such clock,
// which is why it is tried first.
type AuthConfig struct {
	SessionToken  string
	RefreshToken  string
	APIKey        string
	APISecret     string
	CognitoRegion string
	CognitoAppID  string
}

// bearerCache holds the current Cognito access token and its expiry.
// Re-derived lazily by Client.getBearer when within bearerRefreshSkew of
// expiry. seeded records whether the injected SessionToken has been
// considered yet, so an already-expired one is tried once and then skipped
// in favour of the refresh path.
type bearerCache struct {
	mu     sync.Mutex
	token  string
	expiry time.Time
	seeded bool
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
	// A Cognito Bearer works against both API hosts. Processor mode always
	// has one available (SESSION_TOKEN, refreshed as needed); target mode
	// only when API key credentials are configured.
	if c.canBearer() {
		if tok, err := c.getBearer(); err == nil {
			req.Header.Set("Authorization", "Bearer "+tok)
			return
		} else if c.callbackToken == "" {
			// No Callback fallback available, so the request is about to fail
			// with an opaque 403 from the authorizer. Log the real reason.
			slog.Error("no usable bearer token; request will be rejected", "error", err)
		} else {
			slog.Warn("bearer unavailable, falling back to callback auth", "error", err)
		}
	}
	req.Header.Set("Authorization",
		fmt.Sprintf("Callback workflow-service:%s:%s", c.executionRunID, c.callbackToken))
}

// canBearer reports whether any bearer source is configured. Target mode
// with only a callback token has none.
func (c *Client) canBearer() bool {
	if c.auth.SessionToken != "" || c.auth.RefreshToken != "" {
		return true
	}
	// Deliberately does not require CognitoAppID: cognitoClientID discovers
	// it from the unauthenticated cognito-config endpoint. Demanding it here
	// would gate the key/secret path on the very value discovery provides,
	// so a correctly configured key/secret would skip Cognito entirely and
	// fall through to a Callback header with an empty token — surfacing as
	// an opaque 401 from nginx rather than an auth error.
	return c.auth.APIKey != "" && c.auth.APISecret != ""
}

// getBearer returns a live Cognito access token, deriving a fresh one when
// the cached value is missing or within bearerRefreshSkew of expiry.
// Safe for concurrent use. See AuthConfig for the source precedence.
func (c *Client) getBearer() (string, error) {
	c.bearer.mu.Lock()
	defer c.bearer.mu.Unlock()

	if c.bearer.token != "" && time.Now().Before(c.bearer.expiry.Add(-bearerRefreshSkew)) {
		return c.bearer.token, nil
	}

	var errs []error

	// Key/secret first when configured. It is the only source that can be
	// re-derived indefinitely: SESSION_TOKEN is a ~60-minute snapshot taken
	// at run start with nothing writing a refreshed value back, and the
	// refresh token behind it expires relative to the *human login* that
	// produced it (auth_time), not to this run — so a workflow launched from
	// a day-old browser session can begin with both already dead. A
	// key/secret pair has no such clock.
	if c.auth.APIKey != "" && c.auth.APISecret != "" {
		clientID, err := c.keySecretClientID()
		if err != nil {
			errs = append(errs, fmt.Errorf("resolving Cognito app client id for key/secret: %w", err))
		} else {
			tok, expiresIn, err := mintCognitoAccessToken(c.httpClient, c.auth.CognitoRegion, clientID, c.auth.APIKey, c.auth.APISecret)
			if err != nil {
				errs = append(errs, fmt.Errorf("minting Cognito access token: %w", err))
			} else {
				c.bearer.token = tok
				c.bearer.expiry = time.Now().Add(time.Duration(expiresIn) * time.Second)
				slog.Info("minted access token from API key/secret", "expiresIn", expiresIn)
				return tok, nil
			}
		}
	}

	// The injected session token, considered once. Expect it to be expired on
	// any long chain — this stage runs last.
	if !c.bearer.seeded {
		c.bearer.seeded = true
		if c.auth.SessionToken != "" {
			exp, ok := jwtExpiry(c.auth.SessionToken)
			switch {
			case ok && time.Now().Before(exp.Add(-bearerRefreshSkew)):
				c.bearer.token = c.auth.SessionToken
				c.bearer.expiry = exp
				return c.bearer.token, nil
			case !ok && c.auth.RefreshToken == "":
				// Can't read an expiry and have no way to get a new token;
				// use it as-is rather than failing outright.
				slog.Warn("session token has no readable exp claim; using as-is")
				return c.auth.SessionToken, nil
			case ok:
				slog.Info("session token expired or expiring; refreshing", "expiredAt", exp.UTC().Format(time.RFC3339))
			}
		}
	}

	if c.auth.RefreshToken != "" {
		clientID, err := c.cognitoClientID()
		if err != nil {
			errs = append(errs, fmt.Errorf("resolving Cognito app client id: %w", err))
		} else {
			tok, expiresIn, err := refreshCognitoAccessToken(c.httpClient, c.auth.CognitoRegion, clientID, c.auth.RefreshToken)
			if err != nil {
				errs = append(errs, fmt.Errorf("refreshing Cognito access token: %w", err))
			} else {
				c.bearer.token = tok
				c.bearer.expiry = time.Now().Add(time.Duration(expiresIn) * time.Second)
				slog.Info("refreshed session token", "expiresIn", expiresIn)
				return tok, nil
			}
		}
	}

	if len(errs) == 0 {
		return "", fmt.Errorf("no bearer source available: session token expired and neither REFRESH_TOKEN nor PENNSIEVE_API_KEY is set")
	}
	return "", errors.Join(errs...)
}

// keySecretClientID resolves the app client id for a USER_PASSWORD_AUTH
// mint from an API key/secret.
//
// Unlike cognitoClientID this deliberately ignores the session token's
// client_id claim. That token comes from the *user* pool (a human login),
// while an API key/secret is a *token* pool user — borrowing the claim
// would point the mint at the wrong pool and fail as "Incorrect username
// or password". fetchCognitoConfig returns the token pool's client.
func (c *Client) keySecretClientID() (string, error) {
	if c.auth.CognitoAppID != "" {
		return c.auth.CognitoAppID, nil
	}
	region, id, err := fetchCognitoConfig(c.httpClient, c.apiHost)
	if err != nil {
		return "", err
	}
	c.auth.CognitoAppID = id
	if region != "" {
		c.auth.CognitoRegion = region
	}
	return id, nil
}

// cognitoClientID resolves the app client id needed for a Cognito
// InitiateAuth call, in order: configured value, the client_id claim on the
// session token we were handed, then the unauthenticated cognito-config
// endpoint on the legacy API host.
//
// The claim-based step is correct for REFRESH_TOKEN_AUTH specifically: a
// refresh must target whichever pool minted the token, and the token
// records that in client_id.
func (c *Client) cognitoClientID() (string, error) {
	if c.auth.CognitoAppID != "" {
		return c.auth.CognitoAppID, nil
	}
	// A Cognito access token carries the client that issued it, so a
	// processor can refresh without being told the id. Works on an expired
	// token: we decode the claims, we don't validate them.
	if id := jwtClaimString(c.auth.SessionToken, "client_id"); id != "" {
		c.auth.CognitoAppID = id
		return id, nil
	}
	region, id, err := fetchCognitoConfig(c.httpClient, c.apiHost)
	if err != nil {
		return "", err
	}
	c.auth.CognitoAppID = id
	if region != "" {
		c.auth.CognitoRegion = region
	}
	return id, nil
}

// fetchCognitoConfig reads the unauthenticated GET /authentication/cognito-config
// endpoint on the legacy API host, returning (region, appClientId).
func fetchCognitoConfig(httpClient *http.Client, apiHost string) (string, string, error) {
	resp, err := httpClient.Get(apiHost + "/authentication/cognito-config")
	if err != nil {
		return "", "", fmt.Errorf("fetching cognito config: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("reading cognito config: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("cognito config: HTTP %d: %s", resp.StatusCode, string(body))
	}
	var parsed struct {
		Region   string `json:"region"`
		UserPool struct {
			AppClientID string `json:"appClientId"`
		} `json:"userPool"`
		TokenPool struct {
			AppClientID string `json:"appClientId"`
		} `json:"tokenPool"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", "", fmt.Errorf("decoding cognito config: %w", err)
	}

	// API key/secret pairs are users of the token pool; the user pool holds
	// human email/password logins. Authenticating a key against the user
	// pool's client fails as "Incorrect username or password" — that
	// username genuinely does not exist there — which reads as bad
	// credentials rather than a misrouted request. Prefer tokenPool and
	// fall back to userPool for deployments that publish no token pool.
	//
	// The fallback keys off the value, not the field's presence: the real
	// response carries identityPool.appClientId as "", so a presence check
	// would happily hand Cognito an empty client id.
	if id := parsed.TokenPool.AppClientID; id != "" {
		return parsed.Region, id, nil
	}
	if parsed.UserPool.AppClientID == "" {
		return "", "", fmt.Errorf("cognito config has no tokenPool.appClientId or userPool.appClientId")
	}
	return parsed.Region, parsed.UserPool.AppClientID, nil
}

// jwtClaims decodes a JWT's payload without verifying its signature. The
// caller is not authenticating anything — it only needs claims (exp,
// client_id) off a token the orchestrator handed us, which may be expired.
func jwtClaims(token string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("not a JWT: %d segments", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decoding JWT payload: %w", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("unmarshaling JWT claims: %w", err)
	}
	return claims, nil
}

// jwtExpiry returns the token's exp claim. ok is false if the token is
// unparseable or carries no numeric exp.
func jwtExpiry(token string) (time.Time, bool) {
	claims, err := jwtClaims(token)
	if err != nil {
		return time.Time{}, false
	}
	exp, ok := claims["exp"].(float64)
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(int64(exp), 0), true
}

// jwtClaimString returns a string claim, or "" if absent/not a string.
func jwtClaimString(token, claim string) string {
	claims, err := jwtClaims(token)
	if err != nil {
		return ""
	}
	s, _ := claims[claim].(string)
	return s
}

// refreshCognitoAccessToken exchanges a refresh token for a fresh access
// token via Cognito REFRESH_TOKEN_AUTH, returning (accessToken,
// expiresInSeconds). Needs no valid access token and no AWS credentials.
func refreshCognitoAccessToken(httpClient *http.Client, region, clientID, refreshToken string) (string, int, error) {
	return initiateAuth(httpClient, region, map[string]any{
		"AuthFlow": "REFRESH_TOKEN_AUTH",
		"AuthParameters": map[string]string{
			"REFRESH_TOKEN": refreshToken,
		},
		"ClientId": clientID,
	})
}

// mintCognitoAccessToken performs Cognito's USER_PASSWORD_AUTH InitiateAuth
// against the given app client and returns (accessToken, expiresInSeconds).
func mintCognitoAccessToken(httpClient *http.Client, region, clientID, username, password string) (string, int, error) {
	return initiateAuth(httpClient, region, map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": username,
			"PASSWORD": password,
		},
		"ClientId": clientID,
	})
}

// initiateAuth posts a Cognito InitiateAuth body and returns (accessToken,
// expiresInSeconds). InitiateAuth is unauthenticated (requires no AWS creds),
// so we post directly to the cognito-idp endpoint and avoid pulling in the
// AWS SDK service module.
func initiateAuth(httpClient *http.Client, region string, body map[string]any) (string, int, error) {
	endpoint := fmt.Sprintf("https://cognito-idp.%s.amazonaws.com/", region)
	if cognitoEndpointOverride != "" {
		endpoint = cognitoEndpointOverride
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
