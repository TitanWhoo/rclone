package open115

import (
	"context"
	"fmt"
	"net/http"

	"github.com/rclone/rclone/lib/rest"
)

const (
	baseAPI      = "https://proapi.115.com"
	passportAPI  = "https://passportapi.115.com"
	qrcodeAPI    = "https://qrcodeapi.115.com"
	defaultLimit = 1000 // Default limit for listing items
)

var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// client is a wrapper around rest.Client to handle 115 Cloud Drive API calls.
type client struct {
	*rest.Client
	ts *TokenSource
}

// newClient creates a new API client.
func newClient(rc *rest.Client, ts *TokenSource) *client {
	return &client{
		Client: rc,
		ts:     ts,
	}
}

func (c *client) CallJSON(ctx context.Context, opts *rest.Opts, request any, response any) (resp *http.Response, err error) {
	if c.ts == nil {
		return c.Client.CallJSON(ctx, opts, request, response)
	}

	// use access token from TokenSource
	if opts.ExtraHeaders == nil {
		opts.ExtraHeaders = make(map[string]string)
	}
	token, err := c.ts.Token()
	if err != nil {
		return nil, err
	}
	opts.ExtraHeaders["Authorization"] = fmt.Sprintf("Bearer %s", token)
	return c.Client.CallJSON(ctx, opts, request, response)
}
