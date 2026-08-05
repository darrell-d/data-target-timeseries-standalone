package pennsieve

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// testJWT builds an unsigned JWT carrying the given claims. The signature is
// junk on purpose: nothing here verifies it, and the token the orchestrator
// hands us is routinely expired by the time this stage runs.
func testJWT(claims map[string]any) string {
	payload, err := json.Marshal(claims)
	if err != nil {
		panic(err)
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".not-a-real-signature"
}

func TestJWTExpiry(t *testing.T) {
	want := time.Now().Add(37 * time.Minute).Truncate(time.Second)

	got, ok := jwtExpiry(testJWT(map[string]any{"exp": want.Unix()}))
	if !ok {
		t.Fatal("jwtExpiry: ok = false, want true")
	}
	if !got.Equal(want) {
		t.Errorf("jwtExpiry = %v, want %v", got, want)
	}

	for _, tc := range []struct {
		name  string
		token string
	}{
		{"empty", ""},
		{"not a jwt", "just-a-string"},
		{"two segments", "aaa.bbb"},
		{"payload not base64", "aaa.!!!!.ccc"},
		{"no exp claim", testJWT(map[string]any{"client_id": "abc"})},
		{"exp not numeric", testJWT(map[string]any{"exp": "soon"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := jwtExpiry(tc.token); ok {
				t.Errorf("jwtExpiry(%q): ok = true, want false", tc.token)
			}
		})
	}
}

func TestJWTClaimString(t *testing.T) {
	tok := testJWT(map[string]any{"client_id": "5abc123", "exp": 1784320380})

	if got := jwtClaimString(tok, "client_id"); got != "5abc123" {
		t.Errorf("client_id = %q, want %q", got, "5abc123")
	}
	if got := jwtClaimString(tok, "missing"); got != "" {
		t.Errorf("missing claim = %q, want empty", got)
	}
	// exp is a number, not a string.
	if got := jwtClaimString(tok, "exp"); got != "" {
		t.Errorf("exp as string = %q, want empty", got)
	}
	if got := jwtClaimString("garbage", "client_id"); got != "" {
		t.Errorf("garbage token = %q, want empty", got)
	}
}

func TestGetBearerUsesLiveSessionToken(t *testing.T) {
	live := testJWT(map[string]any{"exp": time.Now().Add(time.Hour).Unix()})
	c := newTestClient(AuthConfig{SessionToken: live, RefreshToken: "refresh-me"})

	got, err := c.getBearer()
	if err != nil {
		t.Fatalf("getBearer: %v", err)
	}
	if got != live {
		t.Error("expected the injected session token to be used while still valid")
	}
}

func TestGetBearerRefreshesExpiredSessionToken(t *testing.T) {
	// The case this change exists for: SESSION_TOKEN is minted once when the
	// run starts and never refreshed, so a chain longer than the token's ~60m
	// life hands this stage a dead one.
	expired := testJWT(map[string]any{
		"exp":       time.Now().Add(-2 * time.Hour).Unix(),
		"client_id": "client-from-token",
	})
	fresh := testJWT(map[string]any{"exp": time.Now().Add(time.Hour).Unix()})

	var gotAuthFlow, gotClientID, gotRefreshToken string
	cognito := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			AuthFlow       string            `json:"AuthFlow"`
			AuthParameters map[string]string `json:"AuthParameters"`
			ClientId       string            `json:"ClientId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decoding InitiateAuth body: %v", err)
		}
		gotAuthFlow = body.AuthFlow
		gotClientID = body.ClientId
		gotRefreshToken = body.AuthParameters["REFRESH_TOKEN"]
		_ = json.NewEncoder(w).Encode(map[string]any{
			"AuthenticationResult": map[string]any{"AccessToken": fresh, "ExpiresIn": 3600},
		})
	}))
	defer cognito.Close()

	c := newTestClient(AuthConfig{SessionToken: expired, RefreshToken: "refresh-me"})
	c.httpClient = cognito.Client()
	cognitoEndpointOverride = cognito.URL + "/"
	defer func() { cognitoEndpointOverride = "" }()

	got, err := c.getBearer()
	if err != nil {
		t.Fatalf("getBearer: %v", err)
	}
	if got != fresh {
		t.Error("expected the refreshed token, got something else")
	}
	if gotAuthFlow != "REFRESH_TOKEN_AUTH" {
		t.Errorf("AuthFlow = %q, want REFRESH_TOKEN_AUTH", gotAuthFlow)
	}
	if gotRefreshToken != "refresh-me" {
		t.Errorf("REFRESH_TOKEN = %q, want refresh-me", gotRefreshToken)
	}
	// The client id came off the expired token's claims — no env var needed.
	if gotClientID != "client-from-token" {
		t.Errorf("ClientId = %q, want client-from-token", gotClientID)
	}

	// Second call must be served from cache, not a second Cognito round trip.
	gotAuthFlow = ""
	if _, err := c.getBearer(); err != nil {
		t.Fatalf("second getBearer: %v", err)
	}
	if gotAuthFlow != "" {
		t.Error("expected the cached token to be reused, but Cognito was called again")
	}
}

func TestGetBearerNoSourceAvailable(t *testing.T) {
	expired := testJWT(map[string]any{"exp": time.Now().Add(-time.Hour).Unix()})
	c := newTestClient(AuthConfig{SessionToken: expired})

	if _, err := c.getBearer(); err == nil {
		t.Fatal("expected an error when the session token is expired and nothing can replace it")
	}
}

func TestGetBearerUnreadableExpiryWithoutRefresh(t *testing.T) {
	// No exp to read and no way to get a new token: use what we were given
	// rather than failing before we've tried.
	c := newTestClient(AuthConfig{SessionToken: "opaque-token"})

	got, err := c.getBearer()
	if err != nil {
		t.Fatalf("getBearer: %v", err)
	}
	if got != "opaque-token" {
		t.Errorf("got %q, want the token as-is", got)
	}
}

func TestCanBearer(t *testing.T) {
	cases := []struct {
		name string
		auth AuthConfig
		want bool
	}{
		{"session token", AuthConfig{SessionToken: "t"}, true},
		{"refresh token only", AuthConfig{RefreshToken: "r"}, true},
		{"full api key set", AuthConfig{APIKey: "k", APISecret: "s", CognitoAppID: "c"}, true},
		{"partial api key set", AuthConfig{APIKey: "k", APISecret: "s"}, false},
		{"callback only", AuthConfig{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := newTestClient(tc.auth).canBearer(); got != tc.want {
				t.Errorf("canBearer = %v, want %v", got, tc.want)
			}
		})
	}
}

func newTestClient(auth AuthConfig) *Client {
	return NewClient("https://api.test", "https://api2.test", "run-1", "cb-token", auth)
}
