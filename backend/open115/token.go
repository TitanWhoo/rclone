package open115

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/rclone/rclone/lib/rest"
	"math/big"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/open115/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
)

// ErrQRCodeTimeout is the error returned when QR code scanning times out
var ErrQRCodeTimeout = fmt.Errorf("QR code scanning timeout, please run configuration again")

// OAuth related constants
const (
	tokenExpiryGrace     = 60 * time.Second   // Grace period before token expiry
	tokenRefreshDuration = 3600 * time.Second // Default token validity is 1 hour
	qrCodeTimeout        = 5 * time.Minute    // QR code validity period
	qrCodePollInterval   = 2 * time.Second    // Polling interval
)

// qrCodeStatus represents QR code scanning status
type qrCodeStatus int

// Scanning status
const (
	qrCodeStatusWaiting   qrCodeStatus = 0 // Waiting for scan
	qrCodeStatusScanned   qrCodeStatus = 1 // Scanned, waiting for confirmation
	qrCodeStatusConfirmed qrCodeStatus = 2 // Confirmed
)

// TokenSource is a custom OAuth2 TokenSource implementation
type TokenSource struct {
	name   string           // Remote name
	ctx    context.Context  // Context
	c      *rest.Client     // API client
	token  *api.Token       // Current token
	expiry time.Time        // Token expiry time
	m      configmap.Mapper // Configuration mapper
	mu     sync.RWMutex     // Mutex
}

// NewTokenSource creates a new TokenSource
func NewTokenSource(ctx context.Context, name string, m configmap.Mapper, client *rest.Client) (*TokenSource, error) {
	ts := &TokenSource{
		c:    client,
		ctx:  ctx,
		name: name,
		m:    m,
	}
	// Try to load token from configuration
	err := ts.readToken()
	if err != nil {
		return nil, err
	}
	return ts, nil
}

// readToken reads token from configuration
func (ts *TokenSource) readToken() error {
	tokenJSON, found := ts.m.Get(config.ConfigToken)
	if !found || tokenJSON == "" {
		return fmt.Errorf("token not found, please run 'rclone config reconnect %s:'", ts.name)
	}

	token := &api.Token{}
	err := json.Unmarshal([]byte(tokenJSON), token)
	if err != nil {
		return fmt.Errorf("unable to parse token: %v", err)
	}

	ts.token = token
	// Set expiry time, if not set calculate from current time
	if ts.token.ExpiresAt.IsZero() {
		ts.token.ExpiresAt = time.Now().Add(tokenRefreshDuration)
	}
	ts.expiry = ts.token.ExpiresAt

	return nil
}

// Token gets a valid token, refreshing if necessary
func (ts *TokenSource) Token() (string, error) {
	// First try to check if token is valid using read lock
	ts.mu.RLock()
	hasValidToken := ts.token != nil && !ts.isTokenExpired()
	accessToken := ""
	if hasValidToken {
		accessToken = ts.token.AccessToken
		ts.mu.RUnlock()
		return accessToken, nil
	} else {
		ts.mu.RUnlock()
	}

	// If token is invalid, acquire write lock to refresh
	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Double check to avoid other goroutines refreshing the token while acquiring lock
	if ts.token != nil && !ts.isTokenExpired() {
		return ts.token.AccessToken, nil
	}

	// Refresh token
	err := ts.refreshToken()
	if err != nil {
		return "", err
	}
	return ts.token.AccessToken, nil
}

// isTokenExpired checks if token is expired
func (ts *TokenSource) isTokenExpired() bool {
	if ts.token == nil {
		return true
	}
	return time.Now().Add(tokenExpiryGrace).After(ts.expiry)
}

// refreshToken refreshes the token
func (ts *TokenSource) refreshToken() error {
	if ts.token == nil || ts.token.RefreshToken == "" {
		return fmt.Errorf("no valid refresh token, please run 'rclone config reconnect %s:'", ts.name)
	}
	opts := rest.Opts{
		Method:      "POST",
		RootURL:     passportAPI,
		Path:        "/download/refreshToken",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(fmt.Sprintf("refresh_token=%s", ts.token.RefreshToken)),
	}
	var resp api.TokenResponse
	_, err := ts.c.CallJSON(ts.ctx, &opts, nil, &resp)
	if err != nil || resp.Data.AccessToken == "" {
		return fmt.Errorf("failed to refresh token: %v", err)
	}
	// Update token
	ts.token.AccessToken = resp.Data.AccessToken
	ts.token.RefreshToken = resp.Data.RefreshToken
	ts.expiry = time.Now().Add(time.Duration(resp.Data.ExpiresIn) * time.Second)
	ts.token.ExpiresAt = ts.expiry
	// Save new token to configuration
	err = ts.saveToken()
	if err != nil {
		return fmt.Errorf("failed to save token: %v", err)
	}
	return nil
}

// saveToken saves token to configuration
func (ts *TokenSource) saveToken() error {
	if ts.token == nil {
		return nil
	}

	tokenJSON, err := json.Marshal(ts.token)
	if err != nil {
		return err
	}

	ts.m.Set(config.ConfigToken, string(tokenJSON))
	return nil
}
func (ts *TokenSource) Auth(appId string) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	// Get QR code URL
	authData, err := ts.getAuthURL(ts.ctx, appId)
	if err != nil {
		return err
	}
	// Display QR code URL for user to scan
	fs.Logf(nil, "Please use the 115 mobile app to scan the QR code: %s", authData.QRCode)
	// Wait for user to scan and confirm authorization
	token, err := ts.waitForQRCodeScan(ts.ctx, authData)
	if err != nil {
		return err
	}
	ts.token = token
	return ts.saveToken()
}

// getAuthURL generates QR code URL for user scanning
func (ts *TokenSource) getAuthURL(ctx context.Context, appId string) (authData *api.AuthDeviceCodeData, err error) {
	// Generate random code verifier
	codeVerifier := generateCodeVerifier()

	// Calculate code challenge
	codeChallenge := calculateCodeChallenge(codeVerifier)

	opts := rest.Opts{
		Method:      "POST",
		RootURL:     passportAPI,
		Path:        "/download/authDeviceCode",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(fmt.Sprintf("client_id=%s&code_challenge=%s&code_challenge_method=sha256", appId, codeChallenge)),
	}
	var resp api.AuthDeviceCodeResponse
	_, err = ts.c.CallJSON(ctx, &opts, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to get authorization code: %w", err)
	}
	// Save code verifier to response
	resp.Data.CodeVerifier = codeVerifier
	return &resp.Data, nil
}

// pollQRCodeStatus polls QR code status
func (ts *TokenSource) pollQRCodeStatus(ctx context.Context, authData *api.AuthDeviceCodeData) (qrCodeStatus, error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: qrcodeAPI,
		Path:    "/get/status/",
		Parameters: url.Values{
			"uid":  []string{authData.UID},
			"time": []string{fmt.Sprintf("%d", authData.Time)},
			"sign": []string{authData.Sign},
		},
	}

	var resp api.QRCodeStatusResponse
	_, err := ts.c.CallJSON(ctx, &opts, nil, &resp)
	if err != nil {
		return qrCodeStatusWaiting, err
	}
	return qrCodeStatus(resp.Data.Status), nil
}

// waitForQRCodeScan waits for user to scan QR code and confirm authorization
func (ts *TokenSource) waitForQRCodeScan(ctx context.Context, authData *api.AuthDeviceCodeData) (*api.Token, error) {
	// Set timeout
	deadline := time.Now().Add(qrCodeTimeout)

	for time.Now().Before(deadline) {
		// Check if context is canceled
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Poll QR code status
		status, err := ts.pollQRCodeStatus(ctx, authData)
		if err != nil {
			fs.Logf(nil, "Failed to poll status: %v", err)
		} else {
			switch status {
			case qrCodeStatusConfirmed:
				// Confirmed, get token
				return ts.codeToToken(ctx, authData)
			case qrCodeStatusScanned:
				fs.Logf(nil, "QR code scanned, waiting for authorization confirmation...")
			case qrCodeStatusWaiting:
				fs.Logf(nil, "Waiting for QR code scan...")
			}
		}

		// Wait for a while before polling again
		timer := time.NewTimer(qrCodePollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	return nil, ErrQRCodeTimeout
}

// codeToToken uses authorization code to get token
func (ts *TokenSource) codeToToken(ctx context.Context, authData *api.AuthDeviceCodeData) (*api.Token, error) {
	opts := rest.Opts{
		Method:      "POST",
		RootURL:     passportAPI,
		Path:        "/download/deviceCodeToToken",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(fmt.Sprintf("uid=%s&code_verifier=%s", authData.UID, authData.CodeVerifier)),
	}
	var resp api.DeviceCodeToTokenResponse
	_, err := ts.c.CallJSON(ctx, &opts, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	}
	// Create Token object
	expiresAt := time.Now().Add(time.Duration(resp.Data.ExpiresIn) * time.Second)
	token := &api.Token{
		AccessToken:  resp.Data.AccessToken,
		RefreshToken: resp.Data.RefreshToken,
		ExpiresAt:    expiresAt,
	}
	return token, nil
}

// calculateCodeChallenge calculates the code challenge.
func calculateCodeChallenge(codeVerifier string) string {
	hash := sha256.Sum256([]byte(codeVerifier))
	return base64.StdEncoding.EncodeToString(hash[:])
}

// generateCodeVerifier generates a code verifier
func generateCodeVerifier() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~"
	result := make([]byte, 64)
	length := len(charset)
	// Use cryptographically secure random numbers
	for i := range result {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(length)))
		if err != nil {
			// If crypto random fails, fall back to less random method
			result[i] = charset[i%length]
			continue
		}
		result[i] = charset[num.Int64()]
	}

	return string(result)
}
