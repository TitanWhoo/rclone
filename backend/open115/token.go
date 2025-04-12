package open115

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
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

// QRCodeStatus represents QR code scanning status
type QRCodeStatus int

// Scanning status
const (
	QRCodeStatusWaiting   QRCodeStatus = 0 // Waiting for scan
	QRCodeStatusScanned   QRCodeStatus = 1 // Scanned, waiting for confirmation
	QRCodeStatusConfirmed QRCodeStatus = 2 // Confirmed
)

// TokenSource is a custom OAuth2 TokenSource implementation
type TokenSource struct {
	client *client          // API client
	token  *api.Token       // Current token
	expiry time.Time        // Token expiry time
	ctx    context.Context  // Context
	name   string           // Remote name
	m      configmap.Mapper // Configuration mapper
	mu     sync.RWMutex     // Mutex
}

// NewTokenSource creates a new TokenSource
func NewTokenSource(ctx context.Context, name string, m configmap.Mapper, client *client) (*TokenSource, error) {
	ts := &TokenSource{
		client: client,
		ctx:    ctx,
		name:   name,
		m:      m,
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
	resp, err := ts.client.RefreshToken(ts.ctx, ts.token.RefreshToken)
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

// getAuthURL generates QR code URL for user scanning
func getAuthURL(ctx context.Context, client *client) (authData *api.AuthDeviceCodeData, err error) {
	// Generate random code verifier
	codeVerifier := generateCodeVerifier()

	resp, err := client.AuthDeviceCode(ctx, codeVerifier)
	if err != nil {
		return nil, fmt.Errorf("failed to get authorization code: %w", err)
	}

	// Save code verifier to response
	resp.Data.CodeVerifier = codeVerifier

	return &resp.Data, nil
}

// pollQRCodeStatus polls QR code status
func pollQRCodeStatus(ctx context.Context, client *client, authData *api.AuthDeviceCodeData) (QRCodeStatus, error) {
	resp, err := client.PollQRCodeStatus(ctx, authData.UID, authData.Time, authData.Sign)
	if err != nil {
		return QRCodeStatusWaiting, err
	}

	return QRCodeStatus(resp.Data.Status), nil
}

// waitForQRCodeScan waits for user to scan QR code and confirm authorization
func waitForQRCodeScan(ctx context.Context, client *client, authData *api.AuthDeviceCodeData) (*api.Token, error) {
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
		status, err := pollQRCodeStatus(ctx, client, authData)
		if err != nil {
			fs.Logf(nil, "Failed to poll status: %v", err)
		} else {
			switch status {
			case QRCodeStatusConfirmed:
				// Confirmed, get token
				return codeToToken(ctx, client, authData)
			case QRCodeStatusScanned:
				fs.Logf(nil, "QR code scanned, waiting for authorization confirmation...")
			case QRCodeStatusWaiting:
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
func codeToToken(ctx context.Context, client *client, authData *api.AuthDeviceCodeData) (*api.Token, error) {
	resp, err := client.DeviceCodeToToken(ctx, authData.UID, authData.CodeVerifier)
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
