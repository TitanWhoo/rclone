package api

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// Response represents the basic API response structure.
type Response struct {
	Code    int    `json:"code,omitempty"`    // Code is the return code.
	Message string `json:"message,omitempty"` // Message is the return message.
	Error   string `json:"error,omitempty"`   // Error is the error message.
	Errno   int    `json:"errno,omitempty"`   // Errno is the error number.
}

type AuthDeviceCodeResponse struct {
	Response
	Data AuthDeviceCodeData `json:"data"` // Data contains device code authorization response data.
}

// AuthDeviceCodeData holds the data from a device code authorization response.
type AuthDeviceCodeData struct {
	UID          string `json:"uid,omitempty"`           // UID is the user ID.
	Time         int64  `json:"time,omitempty"`          // Time is the timestamp.
	QRCode       string `json:"qrcode,omitempty"`        // QRCode is the QR code content.
	Sign         string `json:"sign,omitempty"`          // Sign is the signature.
	CodeVerifier string `json:"code_verifier,omitempty"` // CodeVerifier is the code verifier.
}

// QRCodeStatusResponse represents the QR code status response.
type QRCodeStatusResponse struct {
	Response
	Data QRCodeStatusData `json:"data"` // Data contains QR code status data.
}

// QRCodeStatusData holds the data from a QR code status polling response.
type QRCodeStatusData struct {
	Msg     string `json:"msg,omitempty"`     // Msg is the message.
	Status  int    `json:"status,omitempty"`  // Status is the QR code status.
	Version string `json:"version,omitempty"` // Version is the API version.
}

// DeviceCodeToTokenResponse represents the response for exchanging a device code for a token.
type DeviceCodeToTokenResponse struct {
	Response
	Data TokenData `json:"data"` // Data contains the token data.
}

// TokenData holds the access token response data.
type TokenData struct {
	AccessToken  string `json:"access_token,omitempty"`  // AccessToken is the access token.
	RefreshToken string `json:"refresh_token,omitempty"` // RefreshToken is the refresh token.
	ExpiresIn    int    `json:"expires_in,omitempty"`    // ExpiresIn is the expiration time in seconds.
}

// TokenResponse represents the access token response data.
type TokenResponse struct {
	Response
	Data TokenData `json:"data"` // Data contains the token data.
}

// Token holds the token information along with its expiration time.
type Token struct {
	AccessToken  string    `json:"access_token"`  // AccessToken is the access token.
	RefreshToken string    `json:"refresh_token"` // RefreshToken is the refresh token.
	ExpiresAt    time.Time `json:"expires_at"`    // ExpiresAt is the absolute expiration time.
}

// FileLabel represents a file label.
type FileLabel struct {
	ID         string `json:"id"`          // ID is the file label ID.
	Name       string `json:"name"`        // Name is the file label name.
	Sort       string `json:"sort"`        // Sort is the file label sort order.
	Color      string `json:"color"`       // Color is the file label color.
	IsDefault  int    `json:"is_default"`  // IsDefault indicates the type: 0: Recently used; 1: Not recently used; 2: Default label.
	UpdateTime int    `json:"update_time"` // UpdateTime is the update time.
	CreateTime int    `json:"create_time"` // CreateTime is the creation time.
}

// FileInfo represents information about a file or folder.
type FileInfo struct {
	FID      string      `json:"fid"`                 // FID is the file ID.
	AID      string      `json:"aid"`                 // AID is the file status (alias for fid?). 1 Normal, 7 Deleted (Recycle Bin), 120 Permanently Deleted.
	PID      string      `json:"pid"`                 // PID is the parent directory ID.
	FC       json.Number `json:"fc"`                  // FC is the file category. 0 Folder, 1 File.
	FN       string      `json:"fn"`                  // FN is the file/folder name.
	FCO      string      `json:"fco"`                 // FCO is the folder cover.
	ISM      json.Number `json:"ism"`                 // ISM indicates if starred (1: starred).
	ISP      json.Number `json:"isp"`                 // ISP indicates if encrypted (1: encrypted).
	PC       string      `json:"pc"`                  // PC is the file pick code.
	UPT      uint64      `json:"upt"`                 // UPT is the modification time.
	UET      uint64      `json:"uet"`                 // UET is the modification time (duplicate?).
	UPPT     uint64      `json:"uppt"`                // UPPT is the upload time.
	CM       int         `json:"cm"`                  // CM - unknown purpose.
	FDesc    string      `json:"fdesc"`               // FDesc is the file description/remark.
	ISPL     json.Number `json:"ispl"`                // ISPL - toggle for counting video duration in a folder?
	FL       []FileLabel `json:"fl,omitempty"`        // FL is the list of file labels.
	SHA1     string      `json:"sha1,omitempty"`      // SHA1 is the SHA1 hash.
	FS       json.Number `json:"fs,omitempty"`        // FS is the file size.
	FTA      string      `json:"fta,omitempty"`       // FTA is the file status (0/2 Incomplete Upload, 1 Complete Upload).
	ICO      string      `json:"ico,omitempty"`       // ICO is the file extension icon.
	FATR     string      `json:"fatr,omitempty"`      // FATR is the audio length.
	ISV      json.Number `json:"isv,omitempty"`       // ISV indicates if it's a video.
	DEF      json.Number `json:"def,omitempty"`       // DEF is the video definition; 1:SD 2:HD 3:FHD 4:1080P 5:4k; 100:Original.
	DEF2     json.Number `json:"def2,omitempty"`      // DEF2 is the video definition (duplicate?);
	PlayLong json.Number `json:"play_long,omitempty"` // PlayLong is the audio/video duration in seconds.
	VImg     string      `json:"v_img,omitempty"`     // VImg is the video cover image URL.
	Thumb    string      `json:"thumb,omitempty"`     // Thumb is the image thumbnail URL.
	UO       string      `json:"uo,omitempty"`        // UO is the original image URL.
}

// PathInfo represents information about a file path component.
type PathInfo struct {
	Name string      `json:"name"`  // Name is the parent directory name.
	AID  interface{} `json:"aid"`   // AID - unknown purpose.
	CID  interface{} `json:"cid"`   // CID - unknown purpose.
	PID  interface{} `json:"pid"`   // PID - unknown purpose.
	ISP  interface{} `json:"isp"`   // ISP - unknown purpose.
	PCID string      `json:"p_cid"` // PCID - unknown purpose.
	FV   string      `json:"fv"`    // FV - unknown purpose.
}

// FileListResponse represents the response for getting a file list.
type FileListResponse struct {
	Response
	Data       []FileInfo  `json:"data"`             // Data contains the list of files/folders.
	Count      int         `json:"count"`            // Count - unknown purpose.
	SysCount   int         `json:"sys_count"`        // SysCount is the number of system folders.
	Offset     int         `json:"offset"`           // Offset is the starting position.
	Limit      json.Number `json:"limit"`            // Limit is the page size.
	AID        int         `json:"aid"`              // AID is the file status filter.
	CID        int         `json:"cid"`              // CID is the parent directory ID.
	IsAsc      int         `json:"is_asc"`           // IsAsc is the sort order (1: ascending, 0: descending).
	MinSize    int         `json:"min_size"`         // MinSize - unknown purpose.
	MaxSize    int         `json:"max_size"`         // MaxSize - unknown purpose.
	SysDir     string      `json:"sys_dir"`          // SysDir - unknown purpose.
	HideData   string      `json:"hide_data"`        // HideData indicates whether file data is returned.
	RecordTime string      `json:"record_open_time"` // RecordTime indicates whether to record folder open time.
	Star       int         `json:"star"`             // Star filters by star status (1: starred, 0: all).
	Type       int         `json:"type"`             // Type is the primary filter category (1:Doc, 2:Img, 3:Music, 4:Video, 5:Zip, 6:App, 7:Book).
	Suffix     string      `json:"suffix"`           // Suffix is the file extension for the 'other' type filter.
	Path       []PathInfo  `json:"path"`             // Path is the parent directory tree.
	Cur        int         `json:"cur"`              // Cur indicates if only files in the current folder are shown.
	StDir      int         `json:"stdir"`            // StDir indicates if folders are shown when filtering files (1: show, 0: hide).
	Fields     string      `json:"fields"`           // Fields - unknown purpose.
	Order      string      `json:"order"`            // Order is the sort field.
}

// FolderCreateResponse represents the response for creating a folder.
type FolderCreateResponse struct {
	Response
	Data FolderCreateData `json:"data"` // Data contains the created folder information.
}

// FolderCreateData holds the data returned after creating a folder.
type FolderCreateData struct {
	FileName string      `json:"file_name"` // FileName is the name of the created folder.
	FileID   json.Number `json:"file_id"`   // FileID is the ID of the created folder.
}

// FileInfoResponse represents the response for getting file/folder details.
type FileInfoResponse struct {
	Response
	Data FileDetailInfo `json:"data"` // Data contains the detailed file information.
}

// FileDetailInfo holds detailed information about a file or folder.
type FileDetailInfo struct {
	Count        json.Number `json:"count"`          // Count is the total number of files inside.
	Size         string      `json:"size"`           // Size is the total size of the file/folder.
	FolderCount  json.Number `json:"folder_count"`   // FolderCount is the total number of folders inside.
	PlayLong     json.Number `json:"play_long"`      // PlayLong is the video duration in seconds (-1: calculating, otherwise the duration).
	ShowPlayLong json.Number `json:"show_play_long"` // ShowPlayLong indicates if video duration display is enabled.
	PTime        json.Number `json:"ptime"`          // PTime is the upload time.
	UTime        json.Number `json:"utime"`          // UTime is the modification time.
	FileName     string      `json:"file_name"`      // FileName is the file/folder name.
	PickCode     string      `json:"pick_code"`      // PickCode is the file pick code.
	SHA1         string      `json:"sha1"`           // SHA1 is the SHA1 hash.
	FileID       string      `json:"file_id"`        // FileID is the file/folder ID.
	IsMark       string      `json:"is_mark"`        // IsMark indicates if starred.
	OpenTime     int         `json:"open_time"`      // OpenTime is the last opened time.
	FileCategory string      `json:"file_category"`  // FileCategory indicates the type (1: File, 0: Folder).
	Paths        []PathItem  `json:"paths"`          // Paths is the path of the file/folder.
}

// PathItem represents an item in the file path.
type PathItem struct {
	FileID   json.Number `json:"file_id"`   // FileID is the parent directory ID.
	FileName string      `json:"file_name"` // FileName is the parent directory name.
}

// FileDownloadResponse represents the response for getting a file download URL.
type FileDownloadResponse struct {
	Response
	Data map[string]FileDownloadInfo `json:"data"` // Data contains download info, keyed by file ID.
}

// FileDownloadInfo holds information needed to download a file.
type FileDownloadInfo struct {
	FileName string      `json:"file_name"` // FileName is the file name.
	FileSize int         `json:"file_size"` // FileSize is the file size.
	PickCode string      `json:"pick_code"` // PickCode is the file pick code.
	SHA1     string      `json:"sha1"`      // SHA1 is the file SHA1 hash.
	URL      DownloadURL `json:"url"`       // URL contains the actual download URL.
}

// DownloadURL holds the download URL.
type DownloadURL struct {
	URL string `json:"url"` // URL is the file download address.
}

// FileUpdateResponse represents the response for updating file/folder information.
type FileUpdateResponse struct {
	Response
	Data FileUpdateData `json:"data"` // Data contains the updated file information.
}

// FileUpdateData holds the data returned after updating a file/folder.
type FileUpdateData struct {
	FileName string `json:"file_name"` // FileName is the new file/folder name.
	Star     string `json:"star"`      // Star is the new star status.
}

// GetFileListRequest represents the parameters for a get file list request.
type GetFileListRequest struct {
	CID         string `json:"cid,omitempty"`          // CID is the directory ID (parent_id).
	Type        int    `json:"type,omitempty"`         // Type is the file type filter (1:Doc, 2:Img, 3:Music, 4:Video, 5:Zip, 6:App, 7:Book).
	Limit       int    `json:"limit,omitempty"`        // Limit is the number of items to query (default 20, max 1150).
	Offset      int    `json:"offset,omitempty"`       // Offset is the starting position (default 0).
	Suffix      string `json:"suffix,omitempty"`       // Suffix is the file extension filter.
	Asc         int    `json:"asc,omitempty"`          // Asc is the sort order (1: ascending, 0: descending).
	Order       string `json:"o,omitempty"`            // Order is the sort field (file_name, file_size, user_utime, file_type).
	CustomOrder int    `json:"custom_order,omitempty"` // CustomOrder: 1 Use custom sort, ignore memory; 0 Use memory sort, custom invalid; 2 Custom sort, non-folders top.
	StDir       int    `json:"stdir,omitempty"`        // StDir: Show folders when filtering files? (1: show, 0: hide).
	Star        int    `json:"star,omitempty"`         // Star: Filter starred files (1: yes, 0: all).
	Cur         int    `json:"cur,omitempty"`          // Cur: Show only files in the current folder?
	ShowDir     int    `json:"show_dir,omitempty"`     // ShowDir: Show directories? (0 or 1, default 0).
}

// FileOperationResponse represents a basic response for file operations.
type FileOperationResponse struct {
	Response
	Data []string `json:"data"` // Data is an array of returned data (e.g., IDs).
}

// UploadTokenResponse represents the response for getting an upload token.
type UploadTokenResponse struct {
	Response
	Data UploadTokenData `json:"data"` // Data contains the upload token information.
}

// UploadTokenData holds the upload token information.
type UploadTokenData struct {
	Endpoint        string `json:"endpoint"`        // Endpoint is the OSS endpoint.
	AccessKeySecret string `json:"AccessKeySecret"` // AccessKeySecret is the access key secret.
	SecurityToken   string `json:"SecurityToken"`   // SecurityToken is the security token.
	Expiration      string `json:"Expiration"`      // Expiration is the expiration time.
	AccessKeyId     string `json:"AccessKeyId"`     // AccessKeyId is the access key ID.
}

// InitUploadRequest represents the request to initialize an upload.
type InitUploadRequest struct {
	FileName  string `json:"file_name"`           // FileName is the file name.
	FileSize  int64  `json:"file_size"`           // FileSize is the file size in bytes.
	Target    string `json:"target"`              // Target is the upload target convention (e.g., U_1_0).
	FileID    string `json:"fileid"`              // FileID is the file SHA1 hash.
	PreID     string `json:"preid,omitempty"`     // PreID is the SHA1 of the first 128K (optional).
	PickCode  string `json:"pick_code,omitempty"` // PickCode is the upload task key (optional).
	TopUpload int    `json:"topupload,omitempty"` // TopUpload is a flag for scheduling based on file type (optional).
	SignKey   string `json:"sign_key,omitempty"`  // SignKey is needed for secondary authentication (optional).
	SignVal   string `json:"sign_val,omitempty"`  // SignVal is needed for secondary authentication (uppercase, optional).
}

// InitUploadResponse represents the response for initializing an upload.
type InitUploadResponse struct {
	Response
	Data InitUploadData `json:"data"` // Data contains the initialization result.
}

// InitUploadData holds the data returned after initializing an upload.
type InitUploadData struct {
	PickCode  string      `json:"pick_code"`  // PickCode is the unique ID for the upload task (used for resume).
	Status    int         `json:"status"`     // Status: 1: Not fast upload; 2: Fast upload (upload complete).
	SignKey   string      `json:"sign_key"`   // SignKey is the SHA1 identifier for this calculation (secondary auth).
	SignCheck string      `json:"sign_check"` // SignCheck is the local file SHA1 range for this calculation (secondary auth).
	FileID    string      `json:"file_id"`    // FileID is the ID of the new file if fast upload was successful.
	Target    string      `json:"target"`     // Target is the upload target convention.
	Bucket    string      `json:"bucket"`     // Bucket is the upload bucket name.
	Object    string      `json:"object"`     // Object is the OSS object ID.
	Callback  interface{} `json:"callback"`   // Callback contains callback information.
}

// GetCallback parses the Callback field into a Callback struct.
func (d *InitUploadData) GetCallback() (Callback, error) {
	// If it's a Callback struct
	if cb, ok := d.Callback.(map[string]interface{}); ok {
		return Callback{
			Callback:    cb["callback"].(string),
			CallbackVar: cb["callback_var"].(string),
		}, nil
	}
	if d.Callback == nil || (reflect.TypeOf(d.Callback).Kind() == reflect.Slice &&
		reflect.ValueOf(d.Callback).Len() == 0) {
		return Callback{}, nil
	}
	return Callback{}, fmt.Errorf("unsupported callback type: %v", d.Callback)
}

type Callback struct {
	Callback    string `json:"callback"`     // Callback is the callback info after upload.
	CallbackVar string `json:"callback_var"` // CallbackVar are the callback parameters after upload.
}

// ResumeUploadRequest represents the request to resume an upload.
type ResumeUploadRequest struct {
	FileSize int64  `json:"file_size"` // FileSize is the file size in bytes.
	Target   string `json:"target"`    // Target is the upload target convention.
	FileID   string `json:"fileid"`    // FileID is the file SHA1 hash.
	PickCode string `json:"pick_code"` // PickCode is the upload task key.
}

// ResumeUploadResponse represents the response for resuming an upload.
type ResumeUploadResponse struct {
	Response
	Data []ResumeUploadData `json:"data"` // Data contains the resume upload information.
}

// ResumeUploadData holds the data returned when resuming an upload.
type ResumeUploadData struct {
	PickCode    string `json:"pick_code"`    // PickCode is the unique ID for the upload task.
	Target      string `json:"target"`       // Target is the upload target convention.
	Version     string `json:"version"`      // Version is the API version.
	Bucket      string `json:"bucket"`       // Bucket is the upload bucket name.
	Object      string `json:"object"`       // Object is the OSS object ID.
	Callback    string `json:"callback"`     // Callback is the callback info after upload.
	CallbackVar string `json:"callback_var"` // CallbackVar are the callback parameters after upload.
}

// UploadFileRequest aggregates all parameters needed for the upload process.
type UploadFileRequest struct {
	FileName  string // FileName is the file name.
	FileSize  int64  // FileSize is the file size in bytes.
	Target    string // Target is the upload target convention (e.g., U_1_0).
	FileID    string // FileID is the file SHA1 hash.
	PreID     string // PreID is the SHA1 of the first 128K (optional).
	TopUpload int    // TopUpload is a flag for scheduling based on file type (optional).
}

// UploadFileResponse aggregates the results of the upload process.
type UploadFileResponse struct {
	FileID      string               // FileID is the ID of the new file if fast upload was successful.
	PickCode    string               // PickCode is the unique ID for the upload task.
	IsFast      bool                 // IsFast indicates if fast upload was successful.
	InitRes     *InitUploadResponse  // InitRes contains the initialization response.
	UploadToken *UploadTokenResponse // UploadToken contains the upload credentials (only present if actual upload is needed).
}

// UserInfoResponse represents the response for getting user information.
type UserInfoResponse struct {
	Response
	Data UserInfoData `json:"data"` // Data contains the user information.
}

// UserInfoData holds the user information data structure.
type UserInfoData struct {
	UserID      string    `json:"user_id"`       // UserID is the user identifier
	UserName    string    `json:"user_name"`     // UserName is the username
	UserFaceS   string    `json:"user_face_s"`   // UserFaceS is the small-sized user avatar
	UserFaceM   string    `json:"user_face_m"`   // UserFaceM is the medium-sized user avatar
	UserFaceL   string    `json:"user_face_l"`   // UserFaceL is the large-sized user avatar
	RTSpaceInfo SpaceInfo `json:"rt_space_info"` // RTSpaceInfo contains the user's storage space information
	VipInfo     VipInfo   `json:"vip_info"`      // VipInfo contains the user's VIP level information
}

// SpaceInfo represents the user's storage space information.
type SpaceInfo struct {
	AllTotal  SpaceSize `json:"all_total"`  // AllTotal is the user's total storage space
	AllRemain SpaceSize `json:"all_remain"` // AllRemain is the user's remaining storage space
	AllUse    SpaceSize `json:"all_use"`    // AllUse is the user's used storage space
}

// SpaceSize represents storage size information.
type SpaceSize struct {
	Size       json.Number `json:"size"`        // Size is the space size in bytes
	SizeFormat string      `json:"size_format"` // SizeFormat is the formatted space size
}

// VipInfo represents the user's VIP level information.
type VipInfo struct {
	LevelName string      `json:"level_name"` // LevelName is the VIP level name; e.g., Basic Member, Trial VIP, Monthly VIP, Annual VIP, Super VIP, Long-term VIP
	Expire    json.Number `json:"expire"`     // Expire is the expiration timestamp
}
