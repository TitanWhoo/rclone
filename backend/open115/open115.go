// Package open115 provides an interface to the 115网盘 storage system.
// Package open115 provides an interface to the 115 Cloud Storage system.
package open115

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/rclone/rclone/backend/open115/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/pacer"
)

const (
	defaultAppId  = "100196879"
	minSleep      = 100 * time.Millisecond // minSleep is the minimum sleep time between retries.
	maxSleep      = 5 * time.Second        // maxSleep is the maximum sleep time between retries.
	decayConstant = 2                      // decayConstant is the backoff factor.
	rootID        = "0"                    // rootID is the ID of the root directory.
)

// init registers the backend.
func init() {
	Register("open115")
}

// Register registers the backend.
func Register(fName string) {
	fs.Register(&fs.RegInfo{
		Name:        fName,
		Description: "Open 115 Drive",
		NewFs:       NewFs,
		Config: func(ctx context.Context, name string, m configmap.Mapper, config fs.ConfigIn) (*fs.ConfigOut, error) {
			token, _ := m.Get("token")
			var apiToken api.Token
			err := json.Unmarshal([]byte(token), &apiToken)
			if err == nil && apiToken.RefreshToken != "" {
				return &fs.ConfigOut{}, nil
			}
			opt := new(Options)
			err = configstruct.Set(m, opt)
			if err != nil {
				return nil, err
			}
			appId := func() string {
				if opt.AppID != "" {
					return opt.AppID
				}
				return defaultAppId
			}()
			fsClient := fshttp.NewClient(ctx)
			p := fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))
			f := &Fs{
				name:   name,
				opt:    *opt,
				client: newClient(fsClient, p, appId),
			}
			return f.Config(ctx, name, m, config)
		},
		Options: []fs.Option{
			{
				Name:     "app_id",
				Help:     "open115 appid (leave blank to use default)",
				Required: false,
			},
			{
				Name:     "refresh_token",
				Help:     "Refresh Token (instead of use appid to authorize)",
				Required: false,
			},
		},
	})
}

// Options defines the configuration for this backend.
type Options struct {
	AppID string `config:"app_id"` // AppID is the 115 Open Platform Application ID.
}

// Fs represents an 115 drive file system.
type Fs struct {
	name        string             // name is the remote name.
	root        string             // root is the root path.
	opt         Options            // opt stores the configuration options.
	features    *fs.Features       // features caches the optional features.
	client      *client            // client is the API client.
	tokenSource *TokenSource       // tokenSource provides API tokens.
	dirCache    *dircache.DirCache // dirCache caches directory listings.
}

// Object represents an 115 drive file or directory.
type Object struct {
	fs          *Fs       // fs is the parent Fs.
	remote      string    // remote is the remote path.
	id          string    // id is the file ID.
	modTime     time.Time // modTime is the modification time.
	size        int64     // size is the file size.
	sha1        string    // sha1 is the SHA1 hash.
	pickCode    string    // pickCode is the file pick code.
	hasMetaData bool      // hasMetaData indicates if metadata has been set.
}

// ------------------------------------------------------------

// Name returns the name of the remote.
func (f *Fs) Name() string {
	return f.name
}

// Root returns the root path of the remote.
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the FS.
func (f *Fs) String() string {
	return fmt.Sprintf("115 drive: %s", f.name)
}

// Features returns the optional features of this Fs.
func (f *Fs) Features() *fs.Features {
	return f.features
}

// NewFs constructs a new Fs object from the name and root configuration.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	root = strings.Trim(root, "/")
	pc := fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))
	client := newClient(fshttp.NewClient(ctx), pc, opt.AppID)
	f := &Fs{
		name:   name,
		root:   root,
		opt:    *opt,
		client: client,
	}

	tokenSource, err := NewTokenSource(ctx, name, m, client)
	if err != nil {
		return nil, err
	}
	f.tokenSource = tokenSource

	// Set features
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		NoMultiThreading:        true,
	}).Fill(ctx, f)

	// Create the root directory cache
	f.dirCache = dircache.New(root, rootID, f)

	// Find the current root
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, rootID, &tempF)
		tempF.root = newRoot
		// Make new Fs which is the parent
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			// No root so return old f
			return f, nil
		}
		_, err := tempF.newObjectWithInfo(ctx, remote, nil)
		if err != nil {
			if errors.Is(err, fs.ErrorObjectNotFound) {
				// File doesn't exist so return old f
				return f, nil
			}
			return nil, err
		}
		f.features.Fill(ctx, &tempF)
		// XXX: update the old f here instead of returning tempF, since
		// `features` were already filled with functions having *f as a receiver.
		// See https://github.com/rclone/rclone/issues/2182
		f.dirCache = tempF.dirCache
		f.root = tempF.root
		// return an error with a fs which points to the parent
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// FindLeaf finds a file or directory named leaf in the directory directoryID.
func (f *Fs) FindLeaf(ctx context.Context, directoryID, leafName string) (string, bool, error) {
	// Get token
	token, err := f.tokenSource.Token()
	if err != nil {
		return "", false, err
	}
	var nextOffset = 0
	for {
		req := &api.GetFileListRequest{
			CID:     directoryID,
			Limit:   defaultLimit,
			Offset:  nextOffset,
			StDir:   1,
			ShowDir: 1,
			Cur:     1,
		}
		resp, err := f.client.GetFileList(ctx, token, req)
		if err != nil {
			return "", false, err
		}
		for _, item := range resp.Data {
			if item.FN == leafName {
				return item.FID, item.FC == "0", nil // Return ID and whether it's a directory
			}
		}
		// If the returned count is less than the requested count, we have reached the end.
		if len(resp.Data) < defaultLimit {
			break
		}
		nextOffset += defaultLimit
	}
	return "", false, nil
}

// CreateDir creates the directory named dirName in the directory with directoryID.
func (f *Fs) CreateDir(ctx context.Context, dirID, dirName string) (string, error) {
	// Get token
	token, err := f.tokenSource.Token()
	if err != nil {
		return "", err
	}
	// Create the directory
	resp, err := f.client.CreateFolder(ctx, token, dirID, dirName)
	if err != nil {
		return "", err
	}

	return resp.Data.FileID.String(), nil
}

// NewObject finds the Object at remote. It returns fs.ErrorNotFound if the object isn't present.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

// newObjectWithInfo creates an Object from remote and *api.FileInfo.
//
// info can be nil - if so it will be fetched.
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *api.FileInfo) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	if info != nil {
		// Initialize using provided info
		err := o.setMetaData(info)
		if err != nil {
			return nil, err
		}
		return o, nil
	}
	// Find the file
	err := o.readMetaData(ctx)
	if err != nil {
		return nil, err
	}

	return o, nil
}

// List the objects and directories in the specified directory.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	directoryID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}

	token, err := f.tokenSource.Token()
	if err != nil {
		return nil, err
	}

	var nextOffset = 0
	var fileList []api.FileInfo

	// Get all files page by page
	for {
		req := &api.GetFileListRequest{
			CID:     directoryID,
			Limit:   defaultLimit,
			Offset:  nextOffset,
			ShowDir: 1, // Show directories
			StDir:   1, // Show folders when filtering files
			Cur:     1, // Show current directory only
		}

		resp, err := f.client.GetFileList(ctx, token, req)
		if err != nil {
			return nil, err
		}

		fileList = append(fileList, resp.Data...)

		// If the returned count is less than the requested count, we have reached the end.
		if len(resp.Data) < defaultLimit {
			break
		}

		nextOffset += defaultLimit
	}

	entries = make([]fs.DirEntry, 0, len(fileList))
	for _, item := range fileList {
		remote := path.Join(dir, item.FN)
		if item.FC == "0" { // Folder
			// Cache directory ID
			f.dirCache.Put(remote, item.FID)
			d := fs.NewDir(remote, time.Unix(int64(item.UPT), 0))
			entries = append(entries, d)
		} else {
			o, err := f.newObjectWithInfo(ctx, remote, &item)
			if err != nil {
				fs.Debugf(o, "list error parsing file info: %v", err)
				continue // Skip problematic files
			}
			entries = append(entries, o)
		}
	}

	return entries, nil
}

// Put uploads the object
//
// Copy the reader data to the object specified by remote.
//
// The reader provided will be drained of data as it is read.
//
// If src is nil, the source object is unknown.
//
// Returns the new object created and error if any.
//
// See Put in the Operations interface for full documentation.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// Get access token
	token, err := f.tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to get access token: %w", err)
	}
	// Get file path and size
	remote := src.Remote()
	size := src.Size()

	// Check if file exists and remove it if necessary
	obj, err := f.NewObject(ctx, remote)
	if err == nil {
		// File exists, delete it
		err = obj.Remove(ctx)
		if err != nil {
			return nil, err
		}
	} else if !errors.Is(err, fs.ErrorObjectNotFound) {
		return nil, err
	}
	// Find upload target directory, create if necessary
	_, directoryID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}

	// Handle empty files
	if size == 0 {
		// Empty file handling logic
		// TO DO: Implement API for empty file creation
		return nil, fs.ErrorNotImplemented
	}

	// 执行文件上传
	// Execute file upload
	return f.upload(ctx, in, token, remote, directoryID, size)
}

// Mkdir creates the directory.
//
// See Mkdir in the Operations interface for documentation.
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Rmdir removes the directory.
//
// See Rmdir in the Operations interface for documentation.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	// Check if the directory is empty
	token, err := f.tokenSource.Token()
	if err != nil {
		return err
	}

	req := &api.GetFileListRequest{
		CID:     dirID,
		Limit:   1, // We only need to know if there's at least one item
		Offset:  0,
		ShowDir: 1, // Show directories
		StDir:   1, // Show folders when filtering files
		Cur:     1, // Show current directory only
	}

	resp, err := f.client.GetFileList(ctx, token, req)
	if err != nil {
		return err
	}

	if len(resp.Data) > 0 {
		return fs.ErrorDirectoryNotEmpty
	}

	// Delete directory
	_, err = f.client.DeleteFiles(ctx, token, []string{dirID}, "")
	if err != nil {
		return err
	}

	f.dirCache.FlushDir(dir)
	return nil
}

// Precision returns the modification time precision.
func (f *Fs) Precision() time.Duration {
	return fs.ModTimeNotSupported
}

// Hashes returns the supported hash types.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.SHA1)
}

// About gets quota information.
//
// See About in the Operations interface for documentation.
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	// Not implemented yet
	return nil, fs.ErrorNotImplemented
}

// ---------------------------------------------------------------------------

// setMetaData sets the metadata from info.
func (o *Object) setMetaData(info *api.FileInfo) error {
	// Ensure it's not a directory
	if info.FC == "0" {
		return fs.ErrorIsDir
	}

	// Set metadata
	o.id = info.FID
	o.pickCode = info.PC
	o.sha1 = strings.ToLower(info.SHA1)

	// Set size
	size, err := strconv.ParseInt(string(info.FS), 10, 64)
	if err != nil {
		return fmt.Errorf("[setMetaData] failed to parse file size %q: %w", info.FS, err)
	}
	o.size = size

	// Set modification time
	o.modTime = time.Unix(int64(info.UPT), 0)

	o.hasMetaData = true

	return nil
}

// readMetaData gets the metadata for the object.
func (o *Object) readMetaData(ctx context.Context) error {
	leaf, directoryID, err := o.fs.dirCache.FindPath(ctx, o.remote, false)
	if err != nil {
		if errors.Is(err, fs.ErrorDirNotFound) {
			return fs.ErrorObjectNotFound
		}
		return err
	}
	// Get token
	token, err := o.fs.tokenSource.Token()
	if err != nil {
		return err
	}

	// List files in the directory, looking for a match
	req := &api.GetFileListRequest{
		CID:     directoryID,
		Limit:   defaultLimit, // Need to list potentially many files
		Offset:  0,
		ShowDir: 1, // Show directories
		StDir:   1, // Show folders when filtering files
		Cur:     1, // Show current directory only
	}

	var nextOffset = 0
	for {
		req.Offset = nextOffset
		resp, err := o.fs.client.GetFileList(ctx, token, req)
		if err != nil {
			return err
		}

		// 在当前页中查找匹配的文件
		// Search for matching files in the current page
		for _, item := range resp.Data {
			if item.FN == leaf {
				return o.setMetaData(&item)
			}
		}
		// 如果返回的数量少于请求的限制，表示已经到达最后一页
		// If the returned count is less than the requested limit, it means we've reached the last page
		if len(resp.Data) < defaultLimit {
			break
		}
		// 更新下一页的偏移量
		// Update the offset for the next page
		nextOffset += defaultLimit
	}
	return fs.ErrorObjectNotFound
}

// Fs returns the parent Fs.
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Remote returns the remote path.
func (o *Object) Remote() string {
	return o.remote
}

// String returns the remote path.
func (o *Object) String() string {
	return o.remote
}

// ModTime returns the modification time.
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// SetModTime sets the modification time (not supported).
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

// Size returns the file size.
func (o *Object) Size() int64 {
	return o.size
}

// Storable returns true if the object is storable.
func (o *Object) Storable() bool {
	return true
}

// Open opens the file for reading.
//
// See Open in the Object interface for documentation.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	// Get token
	token, err := o.fs.tokenSource.Token()
	if err != nil {
		return nil, err
	}

	// Get download URL
	resp, err := o.fs.client.GetFileDownloadURL(ctx, token, o.pickCode)
	if err != nil {
		return nil, fmt.Errorf("[Open] failed to get download URL: %w", err)
	}

	// Get URL and file ID from response
	var fileInfo api.FileDownloadInfo
	for fileID, info := range resp.Data {
		if fileID == o.id {
			fileInfo = info
			break
		}
	}

	if fileInfo.URL.URL == "" {
		return nil, fmt.Errorf("[Open] could not find download URL for file id %s in API response", o.id)
	}
	return o.fs.client.Open(ctx, fileInfo.URL.URL, options...)
}

// Update updates the file content
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	// Directly call Put to implement the update functionality
	newObj, err := o.fs.Put(ctx, in, src, options...)
	if err != nil {
		return err
	}

	// Type assertion to ensure we can access internal fields
	newO, ok := newObj.(*Object)
	if !ok {
		return fmt.Errorf("object returned is of wrong type")
	}

	// Copy properties from the new object
	*o = *newO

	return nil
}

// Hash returns the requested hash Type.
//
// See Hash in the Object interface for documentation.
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t == hash.SHA1 {
		return o.sha1, nil
	}
	return "", hash.ErrUnsupported
}

// Remove removes the object.
//
// See Remove in the Object interface for documentation.
func (o *Object) Remove(ctx context.Context) error {
	// Get token
	token, err := o.fs.tokenSource.Token()
	if err != nil {
		return err
	}

	// Delete file
	_, err = o.fs.client.DeleteFiles(ctx, token, []string{o.id}, "")
	return err
}

// ---------------------------------------------------------------------------

// DirMove moves srcRemote to dstRemote for directories.
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		return fmt.Errorf("can't move directories across different remotes: %w", fs.ErrorCantMove)
	}

	// Find source directory ID
	srcDirID, err := srcFs.dirCache.FindDir(ctx, srcRemote, false)
	if err != nil {
		return err
	}

	// Find destination parent directory
	dstDir := path.Dir(dstRemote)
	if dstDir == "." || dstDir == "/" {
		dstDir = ""
	}
	dstDirID, err := f.dirCache.FindDir(ctx, dstDir, true)
	if err != nil {
		return err
	}

	// Get token
	token, err := f.tokenSource.Token()
	if err != nil {
		return err
	}

	// Rename or move directory
	if srcFs == f && path.Dir(srcRemote) == path.Dir(dstRemote) {
		// Rename directory
		newName := path.Base(dstRemote)
		_, err = f.client.UpdateFile(ctx, token, srcDirID, map[string]string{
			"file_name": newName,
		})
	} else {
		// Move directory
		_, err = f.client.MoveFiles(ctx, token, []string{srcDirID}, dstDirID)
	}
	if err != nil {
		return err
	}

	// After successful move, update cache
	srcFs.dirCache.FlushDir(srcRemote)
	f.dirCache.FlushDir(dstDir)
	return nil
}

// Copy copies the file.
//
// See Copy in the Operations interface for documentation.
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fmt.Errorf("can't copy across different remotes: %w", fs.ErrorCantCopy)
	}

	// Find destination parent directory
	dstDir := path.Dir(remote)
	if dstDir == "." || dstDir == "/" {
		dstDir = ""
	}
	dstDirID, err := f.dirCache.FindDir(ctx, dstDir, true)
	if err != nil {
		return nil, err
	}

	// Get token
	token, err := f.tokenSource.Token()
	if err != nil {
		return nil, err
	}

	// Copy file
	_, err = f.client.CopyFiles(ctx, token, dstDirID, []string{srcObj.id}, false)
	if err != nil {
		return nil, err
	}

	// Flush directory cache
	f.dirCache.FlushDir(dstDir)

	// Return new object
	return f.NewObject(ctx, remote)
}

// Move moves the file.
//
// See Move in the Operations interface for documentation.
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fmt.Errorf("can't move across different remotes: %w", fs.ErrorCantMove)
	}

	// Find destination parent directory
	dstDir := path.Dir(remote)
	if dstDir == "." || dstDir == "/" {
		dstDir = ""
	}
	dstDirID, err := f.dirCache.FindDir(ctx, dstDir, true)
	if err != nil {
		return nil, err
	}

	// Get token
	token, err := f.tokenSource.Token()
	if err != nil {
		return nil, err
	}

	// Move file
	_, err = f.client.MoveFiles(ctx, token, []string{srcObj.id}, dstDirID)
	if err != nil {
		return nil, err
	}

	// Flush directory cache
	f.dirCache.FlushDir(dstDir)

	// Return new object
	return f.NewObject(ctx, remote)
}

// DirCacheFlush flushes the directory cache.
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// CleanUp cleans up temporary files. Implement this if needed.
func (f *Fs) CleanUp(ctx context.Context) error {
	return nil
}

// OAuth performs the OAuth flow to get a token, implementing config.Configurer.
func (f *Fs) OAuth(ctx context.Context, name string, m configmap.Mapper, oauthConfig *fs.ConfigOut) error {
	// Get QR code URL
	authData, err := getAuthURL(ctx, f.client)
	if err != nil {
		return err
	}

	// Display QR code URL for user to scan
	fs.Logf(nil, "Please use the 115 mobile app to scan the QR code: %s", authData.QRCode)

	// Wait for user to scan and confirm authorization
	token, err := waitForQRCodeScan(ctx, f.client, authData)
	if err != nil {
		return err
	}

	// Save token to config
	tokenJSON, err := json.Marshal(token)
	if err != nil {
		return err
	}
	m.Set("token", string(tokenJSON))

	fs.Logf(nil, "Authorization successful, token saved.")
	return nil
}

// Config handles the configuration process.
func (f *Fs) Config(ctx context.Context, name string, m configmap.Mapper, config fs.ConfigIn) (*fs.ConfigOut, error) {
	switch config.State {
	case "", "choose_auth_type":
		return fs.ConfigChooseExclusiveFixed("choose_auth_type_done", "auth_type", "Select authorization type", []fs.OptionExample{
			{Value: "token", Help: "Authenticate using an existing refresh token"},
			{Value: "auth", Help: "Authenticate using your 115 Cloud Open Platform (requires App ID)"},
		})
	case "choose_auth_type_done":
		if config.Result == "auth" {
			return fs.ConfigGoto(config.Result)
		} else if config.Result == "token" {
			return fs.ConfigInput("token", "Enter your refresh token", "Please enter your refresh token")
		}
	case "token":
		// Use TokenSource to save token
		ts := &TokenSource{
			client: f.client,
			token: &api.Token{
				RefreshToken: config.Result,
			},
			ctx:  ctx,
			m:    m,
			name: name,
		}
		err := ts.refreshToken() // Immediately refresh to validate and get other token parts
		if err != nil {
			return nil, fmt.Errorf("failed to validate/refresh token: %v", err)
		}
		return &fs.ConfigOut{State: ""}, nil
	case "auth":
		// Get QR code URL
		authData, err := getAuthURL(ctx, f.client)
		if err != nil {
			return nil, err
		}

		// Display QR code URL for user to scan
		fs.Logf(nil, "Please use the 115 mobile app to scan the QR code: %s", authData.QRCode)

		// Wait for user to scan and confirm authorization
		token, err := waitForQRCodeScan(ctx, f.client, authData)
		if err != nil {
			return nil, err
		}
		// Use TokenSource to save token
		ts := &TokenSource{
			client: f.client,
			token:  token,
			ctx:    ctx,
			name:   name,
			m:      m,
		}
		// Call saveToken to save the token
		err = ts.saveToken()
		if err != nil {
			return nil, fmt.Errorf("failed to save token: %v", err)
		}
		return &fs.ConfigOut{State: ""}, nil
	}
	return nil, fmt.Errorf("unknown config state %q", config.State)
}

// parseSignCheckRange parses the secondary authentication range
func parseSignCheckRange(signCheck string) (start, end int64, err error) {
	parts := strings.Split(signCheck, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid sign_check format: %s", signCheck)
	}

	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return start, end, nil
}

// getOssRegion extracts region from OSS endpoint
func getOssRegion(endpoint string) string {
	if strings.HasPrefix(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
	} else if strings.HasPrefix(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
	}

	parts := strings.Split(endpoint, ".")
	if len(parts) >= 1 {
		regionPart := parts[0]
		if strings.HasPrefix(regionPart, "oss-") {
			return strings.TrimPrefix(regionPart, "oss-")
		}
	}

	return "cn-hangzhou" // default region
}

// uploadToOSS uploads a file to Alibaba Cloud OSS
func uploadToOSS(ctx context.Context, in io.Reader, initData api.InitUploadData, token api.UploadTokenData) error {
	// Create OSS client
	provider := credentials.NewStaticCredentialsProvider(token.AccessKeyId, token.AccessKeySecret, token.SecurityToken)
	cfg := oss.LoadDefaultConfig().
		WithRegion(getOssRegion(token.Endpoint)).
		WithEndpoint(token.Endpoint).
		WithCredentialsProvider(provider).
		WithConnectTimeout(10 * time.Second).
		WithReadWriteTimeout(30 * time.Second).
		WithRetryMaxAttempts(5)
	ossClient := oss.NewClient(cfg)

	// Parse callback data
	callback, err := initData.GetCallback()
	if err != nil {
		return err
	}

	// Base64 encode callback data
	callbackStr := base64.StdEncoding.EncodeToString([]byte(callback.Callback))
	callbackVarStr := base64.StdEncoding.EncodeToString([]byte(callback.CallbackVar))

	// Perform upload
	result, err := ossClient.PutObject(ctx, &oss.PutObjectRequest{
		Bucket:      oss.Ptr(initData.Bucket),
		Key:         oss.Ptr(initData.Object),
		Callback:    oss.Ptr(callbackStr),
		CallbackVar: oss.Ptr(callbackVarStr),
		Body:        in,
	})
	if err != nil {
		return fmt.Errorf("failed to upload to OSS %w", err)
	}
	res, _ := json.Marshal(result)
	fs.Debugf(nil, "uploaded %s to OSS, response: %s", initData.Object, res)
	if code, ok := result.CallbackResult["code"]; !ok || fmt.Sprintf("%v", code) != "0" {
		return fmt.Errorf("callback error %s", res)
	}
	return err
}

// ReadSeekerFile implements a file-like interface wrapping io.ReadSeeker
type ReadSeekerFile struct {
	rs     io.ReadSeeker
	closed bool
}

// Read implements io.Reader
func (f *ReadSeekerFile) Read(p []byte) (n int, err error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	return f.rs.Read(p)
}

// Seek implements io.Seeker
func (f *ReadSeekerFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	return f.rs.Seek(offset, whence)
}

// Close implements io.Closer
func (f *ReadSeekerFile) Close() error {
	if f.closed {
		return os.ErrClosed
	}
	f.closed = true
	if closer, ok := f.rs.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// getIOReadSeekerFromReader attempts to get an io.ReadSeeker from an io.Reader
func getIOReadSeekerFromReader(in io.Reader, size int64) (rs io.ReadSeeker, cleanup func(), err error) {
	// Empty cleanup function
	cleanup = func() {}

	// Check if already a ReadSeeker
	if rs, ok := in.(io.ReadSeeker); ok {
		return rs, cleanup, nil
	}

	// Create temporary file
	tempFile, err := os.CreateTemp("", "rclone-open115-upload-*")
	if err != nil {
		return nil, cleanup, fmt.Errorf("failed to create temporary file: %w", err)
	}

	// Setup cleanup function
	cleanup = func() {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
	}

	// Copy data to temporary file
	written, err := io.Copy(tempFile, in)
	if err != nil {
		cleanup()
		return nil, func() {}, fmt.Errorf("failed to copy data to temporary file: %w", err)
	}

	// Check written size
	if size >= 0 && written != size {
		cleanup()
		return nil, func() {}, fmt.Errorf("failed to copy all data to temporary file: written %d, expected %d", written, size)
	}

	// Reset file position
	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		cleanup()
		return nil, func() {}, fmt.Errorf("failed to seek temporary file: %w", err)
	}

	return tempFile, cleanup, nil
}

// calculateSHA1 calculates the SHA1 hash of data
func calculateSHA1(r io.Reader) (string, error) {
	h := sha1.New()
	_, err := io.Copy(h, r)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// calculateSHA1FromReadSeeker calculates the SHA1 hash of a ReadSeeker
func calculateSHA1FromReadSeeker(rs io.ReadSeeker) (string, error) {
	// Save current position
	currentPos, err := rs.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", fmt.Errorf("failed to get current position: %w", err)
	}

	// Ensure position is restored when function returns
	defer func() {
		_, _ = rs.Seek(currentPos, io.SeekStart)
	}()

	// Calculate SHA1 from beginning
	_, err = rs.Seek(0, io.SeekStart)
	if err != nil {
		return "", fmt.Errorf("failed to seek to start: %w", err)
	}

	return calculateSHA1(rs)
}

// initializeUpload initializes the upload process
func (f *Fs) initializeUpload(ctx context.Context, accessToken, remote, directoryID string, size int64, fileSHA1 string, reader io.ReadSeeker) (*api.InitUploadData, error) {
	// Build upload initialization request
	initReq := &api.InitUploadRequest{
		FileName: path.Base(remote),
		FileSize: size,
		Target:   "U_1_" + directoryID, // Format: U_1_dirID
		FileID:   fileSHA1,
	}

	// Execute upload initialization request
	initResp, err := f.client.InitUpload(ctx, accessToken, initReq)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize upload: %w", err)
	}

	initData := initResp.Data
	if initData.Status == 6 || initData.Status == 8 {
		return nil, errors.New("failed to initialize upload: sign error")
	}

	// Check if secondary authentication is required
	if initData.Status == 7 {
		// Parse authentication range
		start, end, err := parseSignCheckRange(initData.SignCheck)
		if err != nil {
			return nil, fmt.Errorf("failed to parse sign check range: %w", err)
		}

		// Reset reader position to the authentication position
		_, err = reader.Seek(start, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to sign check start position: %w", err)
		}

		// Calculate SHA1 for the specified range
		checkLength := end - start + 1
		signSHA1, err := calculateSHA1Range(reader, checkLength)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate sign check SHA1: %w", err)
		}

		// Convert to uppercase
		sha1Value := strings.ToUpper(signSHA1)

		// Rebuild initialization request with authentication info
		initReq.SignKey = initData.SignKey
		initReq.SignVal = sha1Value

		// Resend initialization request
		initResp, err = f.client.InitUpload(ctx, accessToken, initReq)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize upload with authentication: %w", err)
		}
		initData = initResp.Data
	}
	return &initData, nil
}

// calculateSHA1Range calculates SHA1 hash for a specific length of data from a reader
func calculateSHA1Range(r io.Reader, size int64) (string, error) {
	h := sha1.New()
	n, err := io.CopyN(h, r, size)
	if err != nil && err != io.EOF {
		return "", err
	}
	if n != size && err != io.EOF {
		return "", fmt.Errorf("failed to read %d bytes, only got %d", size, n)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// prepareFileForUpload prepares a file for upload, calculating SHA1 and returning necessary info
func prepareFileForUpload(in io.Reader, size int64) (reader io.ReadSeeker, sha1Hash string, cleanup func(), err error) {
	// Get ReadSeeker
	reader, cleanup, err = getIOReadSeekerFromReader(in, size)
	if err != nil {
		return nil, "", func() {}, err
	}

	// Calculate SHA1
	sha1Hash, err = calculateSHA1FromReadSeeker(reader)
	if err != nil {
		cleanup()
		return nil, "", func() {}, fmt.Errorf("failed to calculate SHA1: %w", err)
	}

	// Reset position
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		cleanup()
		return nil, "", func() {}, fmt.Errorf("failed to seek to start: %w", err)
	}

	return reader, sha1Hash, cleanup, nil
}

// upload handles the file upload process
func (f *Fs) upload(ctx context.Context, in io.Reader, accessToken string, remote string,
	directoryID string, size int64) (fs.Object, error) {

	// Handle empty files
	if size == 0 {
		return nil, fs.ErrorNotImplemented
	}

	// Prepare file for upload
	reader, fileSHA1, cleanup, err := prepareFileForUpload(in, size)
	if err != nil {
		return nil, err
	}
	// Ensure cleanup runs when function exits
	defer cleanup()

	// Initialize upload
	initData, err := f.initializeUpload(ctx, accessToken, remote, directoryID, size, fileSHA1, reader)
	if err != nil {
		fs.Errorf(nil, "failed to initialize upload: %+v", err)
		return nil, err
	}

	// Check if fast upload succeeded
	if initData.Status == 2 {
		fs.Debugf(f, "Fast upload successful for %s, file ID: %s", remote, initData.FileID)
		// Create and return new object
		return f.newObjectWithInfo(ctx, remote, &api.FileInfo{
			FID:  initData.FileID,
			FN:   path.Base(remote),
			PC:   initData.PickCode,
			FS:   json.Number(fmt.Sprintf("%d", size)),
			UPT:  uint64(time.Now().Unix()),
			SHA1: fileSHA1,
		})
	}

	// Get upload token for OSS
	tokenResp, err := f.client.GetUploadToken(ctx, accessToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get upload token: %w", err)
	}

	// Reset file position for upload
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	// Upload file to OSS
	err = uploadToOSS(ctx, reader, *initData, tokenResp.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to upload file to OSS: %w", err)
	}

	// Create and return new object
	return f.newObjectWithInfo(ctx, remote, &api.FileInfo{
		FID:  initData.FileID,
		FN:   path.Base(remote),
		PC:   initData.PickCode,
		FS:   json.Number(fmt.Sprintf("%d", size)),
		UPT:  uint64(time.Now().Unix()),
		SHA1: fileSHA1,
	})
}

// Interfaces implementation check
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ dircache.DirCacher = (*Fs)(nil)
)
