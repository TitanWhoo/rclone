package open115

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"golang.org/x/time/rate"

	"github.com/rclone/rclone/backend/open115/api"

	"github.com/rclone/rclone/lib/rest"
)

const (
	baseAPI      = "https://proapi.115.com"
	passportAPI  = "https://passportapi.115.com"
	qrcodeAPI    = "https://qrcodeapi.115.com"
	defaultLimit = 1000 // Default limit for listing items
)

// client is the 115 Cloud Drive API client.
type client struct {
	*rest.Client
	pacer   *fs.Pacer
	limiter *rate.Limiter
	appID   string
}

var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// newClient creates a new API client.
func newClient(httpClient *http.Client, pacer *fs.Pacer, appID string) *client {
	restClient := rest.NewClient(httpClient)
	return &client{
		Client:  restClient,
		pacer:   pacer,
		limiter: rate.NewLimiter(rate.Limit(10), 20),
		appID:   appID,
	}
}

func (c *client) Open(ctx context.Context, url string, options ...fs.OpenOption) (io.ReadCloser, error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: url,
	}
	opts.Options = options
	var resp *http.Response
	err := c.pacer.Call(func() (bool, error) {
		var err error
		resp, err = c.Call(ctx, &opts)
		return shouldRetry(ctx, resp, nil, err)
	})
	return resp.Body, err
}

// AuthDeviceCode gets the device authorization code and QR code.
func (c *client) AuthDeviceCode(ctx context.Context, codeVerifier string) (*api.AuthDeviceCodeResponse, error) {
	// Calculate code challenge
	codeChallenge := calculateCodeChallenge(codeVerifier)

	opts := rest.Opts{
		Method:      "POST",
		RootURL:     passportAPI,
		Path:        "/open/authDeviceCode",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(fmt.Sprintf("client_id=%s&code_challenge=%s&code_challenge_method=sha256", c.appID, codeChallenge)),
	}

	var resp api.AuthDeviceCodeResponse
	_, err := c.CallJSON(ctx, &opts, nil, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// PollQRCodeStatus polls the QR code status.
func (c *client) PollQRCodeStatus(ctx context.Context, uid string, timestamp int64, sign string) (*api.QRCodeStatusResponse, error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: qrcodeAPI,
		Path:    "/get/status/",
		Parameters: url.Values{
			"uid":  []string{uid},
			"time": []string{fmt.Sprintf("%d", timestamp)},
			"sign": []string{sign},
		},
	}

	var resp api.QRCodeStatusResponse
	_, err := c.CallJSON(ctx, &opts, nil, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// DeviceCodeToToken exchanges the device code for a token.
func (c *client) DeviceCodeToToken(ctx context.Context, uid string, codeVerifier string) (*api.DeviceCodeToTokenResponse, error) {
	opts := rest.Opts{
		Method:      "POST",
		RootURL:     passportAPI,
		Path:        "/open/deviceCodeToToken",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(fmt.Sprintf("uid=%s&code_verifier=%s", uid, codeVerifier)),
	}

	var resp api.DeviceCodeToTokenResponse
	_, err := c.CallJSON(ctx, &opts, nil, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// RefreshToken refreshes the access token.
func (c *client) RefreshToken(ctx context.Context, refreshToken string) (*api.TokenResponse, error) {
	opts := rest.Opts{
		Method:      "POST",
		RootURL:     passportAPI,
		Path:        "/open/refreshToken",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(fmt.Sprintf("refresh_token=%s", refreshToken)),
	}

	var resp api.TokenResponse
	_, err := c.CallJSON(ctx, &opts, nil, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// CreateFolder creates a new folder.
func (c *client) CreateFolder(ctx context.Context, accessToken string, pid string, fileName string) (*api.FolderCreateResponse, error) {
	opts := rest.Opts{
		Method:      "POST",
		RootURL:     baseAPI,
		Path:        "/open/folder/add",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(fmt.Sprintf("pid=%s&file_name=%s", pid, fileName)),
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + accessToken,
		},
	}
	var resp api.FolderCreateResponse
	err := c.pacer.Call(func() (bool, error) {
		r, err := c.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetFileList gets the list of files and folders.
func (c *client) GetFileList(ctx context.Context, accessToken string, req *api.GetFileListRequest) (*api.FileListResponse, error) {
	// Build query parameters
	params := url.Values{}
	if req.CID != "" {
		params.Set("cid", req.CID)
	}
	if req.Type != 0 {
		params.Set("type", fmt.Sprintf("%d", req.Type))
	}
	if req.Limit > 0 {
		params.Set("limit", fmt.Sprintf("%d", req.Limit))
	}
	if req.Offset > 0 {
		params.Set("offset", fmt.Sprintf("%d", req.Offset))
	}
	if req.Suffix != "" {
		params.Set("suffix", req.Suffix)
	}
	if req.Asc != 0 {
		params.Set("asc", fmt.Sprintf("%d", req.Asc))
	}
	if req.Order != "" {
		params.Set("o", req.Order)
	}
	if req.CustomOrder != 0 {
		params.Set("custom_order", fmt.Sprintf("%d", req.CustomOrder))
	}
	if req.StDir != 0 {
		params.Set("stdir", fmt.Sprintf("%d", req.StDir))
	}
	if req.Star != 0 {
		params.Set("star", fmt.Sprintf("%d", req.Star))
	}
	if req.Cur != 0 {
		params.Set("cur", fmt.Sprintf("%d", req.Cur))
	}
	if req.ShowDir != 0 {
		params.Set("show_dir", fmt.Sprintf("%d", req.ShowDir))
	}

	opts := rest.Opts{
		Method:     "GET",
		RootURL:    baseAPI,
		Path:       "/open/ufile/files",
		Parameters: params,
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + accessToken,
		},
	}

	var resp api.FileListResponse
	err := c.pacer.Call(func() (bool, error) {
		_ = c.limiter.Wait(ctx)
		r, err := c.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetFileInfo gets the details of a file or folder.
func (c *client) GetFileInfo(ctx context.Context, accessToken string, fileID string) (*api.FileInfoResponse, error) {
	opts := rest.Opts{
		Method:     "GET",
		RootURL:    baseAPI,
		Path:       "/open/folder/get_info",
		Parameters: url.Values{"file_id": []string{fileID}},
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + accessToken,
		},
	}

	var resp api.FileInfoResponse
	err := c.pacer.Call(func() (bool, error) {
		_ = c.limiter.Wait(ctx)
		r, err := c.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetFileDownloadURL gets the download URL for a file.
func (c *client) GetFileDownloadURL(ctx context.Context, accessToken string, pickCode string) (*api.FileDownloadResponse, error) {
	opts := rest.Opts{
		Method:      "POST",
		RootURL:     baseAPI,
		Path:        "/open/ufile/downurl",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(fmt.Sprintf("pick_code=%s", pickCode)),
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + accessToken,
		},
	}

	var resp api.FileDownloadResponse
	err := c.pacer.Call(func() (bool, error) {
		r, err := c.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// DeleteFiles deletes files or folders.
func (c *client) DeleteFiles(ctx context.Context, accessToken string, fileIDs []string, parentID string) (*api.FileOperationResponse, error) {
	formData := fmt.Sprintf("file_ids=%s", strings.Join(fileIDs, ","))
	if parentID != "" {
		formData += fmt.Sprintf("&parent_id=%s", parentID)
	}

	opts := rest.Opts{
		Method:      "POST",
		RootURL:     baseAPI,
		Path:        "/open/ufile/delete",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(formData),
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + accessToken,
		},
	}

	var resp api.FileOperationResponse
	err := c.pacer.Call(func() (bool, error) {
		r, err := c.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// UpdateFile updates file information (rename or star).
func (c *client) UpdateFile(ctx context.Context, accessToken string, fileID string, options map[string]string) (*api.FileUpdateResponse, error) {
	formData := fmt.Sprintf("file_id=%s", fileID)
	for key, value := range options {
		formData += fmt.Sprintf("&%s=%s", key, value)
	}

	opts := rest.Opts{
		Method:      "POST",
		RootURL:     baseAPI,
		Path:        "/open/ufile/update",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(formData),
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + accessToken,
		},
	}

	var resp api.FileUpdateResponse
	err := c.pacer.Call(func() (bool, error) {
		r, err := c.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// CopyFiles copies files.
func (c *client) CopyFiles(ctx context.Context, accessToken string, pid string, fileIDs []string, noDuplicate bool) (*api.FileOperationResponse, error) {
	formData := fmt.Sprintf("pid=%s&file_id=%s", pid, strings.Join(fileIDs, ","))
	if noDuplicate {
		formData += "&nodupli=1"
	}

	opts := rest.Opts{
		Method:      "POST",
		RootURL:     baseAPI,
		Path:        "/open/ufile/copy",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(formData),
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + accessToken,
		},
	}

	var resp api.FileOperationResponse
	err := c.pacer.Call(func() (bool, error) {
		r, err := c.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// MoveFiles moves files.
func (c *client) MoveFiles(ctx context.Context, accessToken string, fileIDs []string, toCID string) (*api.FileOperationResponse, error) {
	formData := fmt.Sprintf("file_ids=%s&to_cid=%s", strings.Join(fileIDs, ","), toCID)

	opts := rest.Opts{
		Method:      "POST",
		RootURL:     baseAPI,
		Path:        "/open/ufile/move",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(formData),
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + accessToken,
		},
	}

	var resp api.FileOperationResponse
	err := c.pacer.Call(func() (bool, error) {
		r, err := c.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetUploadToken 获取上传凭证
// GetUploadToken gets the upload token
func (c *client) GetUploadToken(ctx context.Context, accessToken string) (*api.UploadTokenResponse, error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: baseAPI,
		Path:    "/open/upload/get_token",
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + accessToken,
		},
	}

	var resp api.UploadTokenResponse
	err := c.pacer.Call(func() (bool, error) {
		r, err := c.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// InitUpload 初始化文件上传
// InitUpload initializes file upload
func (c *client) InitUpload(ctx context.Context, accessToken string, req *api.InitUploadRequest) (*api.InitUploadResponse, error) {
	// Build form data
	formData := url.Values{}
	formData.Set("file_name", req.FileName)
	formData.Set("file_size", fmt.Sprintf("%d", req.FileSize))
	formData.Set("target", req.Target)
	formData.Set("fileid", req.FileID)

	if req.PreID != "" {
		formData.Set("preid", req.PreID)
	}
	if req.PickCode != "" {
		formData.Set("pick_code", req.PickCode)
	}
	if req.TopUpload != 0 {
		formData.Set("topupload", fmt.Sprintf("%d", req.TopUpload))
	}
	if req.SignKey != "" {
		formData.Set("sign_key", req.SignKey)
	}
	if req.SignVal != "" {
		formData.Set("sign_val", req.SignVal)
	}

	opts := rest.Opts{
		Method:      "POST",
		RootURL:     baseAPI,
		Path:        "/open/upload/init",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(formData.Encode()),
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + accessToken,
		},
	}

	var resp api.InitUploadResponse
	err := c.pacer.Call(func() (bool, error) {
		r, err := c.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// ResumeUpload 断点续传
// ResumeUpload handles resumable upload
func (c *client) ResumeUpload(ctx context.Context, accessToken string, req *api.ResumeUploadRequest) (*api.ResumeUploadResponse, error) {
	// Build form data
	formData := url.Values{}
	formData.Set("file_size", fmt.Sprintf("%d", req.FileSize))
	formData.Set("target", req.Target)
	formData.Set("fileid", req.FileID)
	formData.Set("pick_code", req.PickCode)

	opts := rest.Opts{
		Method:      "POST",
		RootURL:     baseAPI,
		Path:        "/open/upload/resume",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(formData.Encode()),
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + accessToken,
		},
	}

	var resp api.ResumeUploadResponse
	err := c.pacer.Call(func() (bool, error) {
		r, err := c.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func shouldRetry(ctx context.Context, res *http.Response, resp *api.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if resp != nil && resp.Errno != 0 {
		return false, fmt.Errorf("API error: code=%d, message=%s", resp.Errno, resp.Error)
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(res, retryErrorCodes), err
}

// calculateCodeChallenge calculates the code challenge.
func calculateCodeChallenge(codeVerifier string) string {
	hash := sha256.Sum256([]byte(codeVerifier))
	return base64.StdEncoding.EncodeToString(hash[:])
}
