// Package open115 provides an interface to the 115 Cloud Storage
package open115

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/lib/oauthutil"

	"github.com/rclone/rclone/fs/fserrors"

	"github.com/rclone/rclone/lib/rest"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
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
	defaultAppId  = "100196955"            // default app id for rclone
	minSleep      = 100 * time.Millisecond // minSleep is the minimum sleep time between retries.
	maxSleep      = 5 * time.Second        // maxSleep is the maximum sleep time between retries.
	decayConstant = 2                      // decayConstant is the backoff factor.
	rootID        = "0"                    // rootID is the ID of the root directory.
	objectDir     = "0"
	MB            = 1024 * 1024
	GB            = 1024 * MB
	TB            = 1024 * GB
)

// calPartSize calculates the part size for multipart upload based on file size
func calPartSize(fileSize int64) int64 {
	var partSize int64 = 20 * MB
	if fileSize > partSize {
		if fileSize > 1*TB { // file Size over 1TB
			partSize = 5 * GB // file part size 5GB
		} else if fileSize > 768*GB { // over 768GB
			partSize = 109951163 // ≈ 104.8576MB, split 1TB into 10,000 part
		} else if fileSize > 512*GB { // over 512GB
			partSize = 82463373 // ≈ 78.6432MB
		} else if fileSize > 384*GB { // over 384GB
			partSize = 54975582 // ≈ 52.4288MB
		} else if fileSize > 256*GB { // over 256GB
			partSize = 41231687 // ≈ 39.3216MB
		} else if fileSize > 128*GB { // over 128GB
			partSize = 27487791 // ≈ 26.2144MB
		}
	}
	return partSize
}

// init registers the backend.
func init() {
	Register("open115")
}

// Register registers the backend.
func Register(fName string) {
	fs.Register(&fs.RegInfo{
		Name:        fName,
		Description: "Open 115 Cloud Drive",
		NewFs:       NewFs,
		Config: func(ctx context.Context, name string, m configmap.Mapper, config fs.ConfigIn) (*fs.ConfigOut, error) {
			fc := fshttp.NewClient(ctx)
			rc := rest.NewClient(fc)
			opt := new(Options)
			err := configstruct.Set(m, opt)
			if err != nil {
				return nil, err
			}
			f := &Fs{
				name:   name,
				opt:    *opt,
				client: newClient(rc, nil),
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
				Help:     "Refresh Token (use token instead of appid to authorize)",
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
	pacer       *fs.Pacer          // pacer is the pacer for this Fs.
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

// Name returns the name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root returns the root path of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("115 drive: %s", f.name)
}

// Features returns the optional features of this Fs
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
	fc := fshttp.NewClient(ctx)
	rc := rest.NewClient(fc)
	tokenSource, err := NewTokenSource(ctx, name, m, rc)
	if err != nil {
		return nil, err
	}
	c := newClient(rc, tokenSource)
	f := &Fs{
		name:   name,
		root:   root,
		opt:    *opt,
		pacer:  fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		client: c,
	}
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

	var nextOffset = 0
	for {
		resp, err := f.getFileList(ctx, directoryID, defaultLimit, nextOffset)
		if err != nil {
			return "", false, err
		}
		for _, item := range resp.Data {
			if item.FN == leafName {
				return item.FID, item.FC == objectDir, nil // Return ID and whether it's a directory
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
	// Create the directory
	resp, err := f.createFolder(ctx, dirID, dirName)
	if err != nil {
		// Check if it's an error that the directory already exists
		// If so, try to find the existing directory
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "exist") {
			// Try to find the existing directory
			existingID, found, findErr := f.FindLeaf(ctx, dirID, dirName)
			if findErr == nil && found {
				return existingID, nil
			}
		}
		return "", err
	}

	return resp.Data.FileID.String(), nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	directoryID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}

	var nextOffset = 0
	var fileList []api.FileInfo

	// Get all files page by page
	for {
		resp, err := f.getFileList(ctx, directoryID, defaultLimit, nextOffset)
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
		if item.FC == objectDir { // Folder
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

// createObject creates a new Object for upload
//
// Used to create new objects
func (f *Fs) createObject(ctx context.Context, remote string, modTime time.Time, size int64) (o *Object, leaf string, directoryID string, err error) {
	// Create the directory for the object if it doesn't exist
	leaf, directoryID, err = f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, leaf, directoryID, err
	}
	// Temporary Object under construction
	o = &Object{
		fs:     f,
		remote: remote,
	}
	return o, leaf, directoryID, nil
}

// Put uploads the object
//
// Copy the reader data to the object specified by remote.
//
// It returns the object created and an error, if any.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// Check if file exists and remove it if necessary
	existingObj, err := f.NewObject(ctx, src.Remote())
	if err == nil {
		// File exists, delete it
		err = existingObj.Remove(ctx)
		if err != nil {
			return nil, err
		}
	} else if !errors.Is(err, fs.ErrorObjectNotFound) {
		return nil, err
	}

	// Use PutUnchecked to upload the file
	return f.PutUnchecked(ctx, in, src, options...)
}

// PutUnchecked uploads the object without checking if it exists
//
// This will create a duplicate if the object already exists.
//
// Copy the reader data to the object specified by remote.
//
// It returns the object created and an error, if any.
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// Get file path and size
	remote := src.Remote()
	size := src.Size()
	modTime := src.ModTime(ctx)

	// Create object and ensure directory exists
	_, _, directoryID, err := f.createObject(ctx, remote, modTime, size)
	if err != nil {
		return nil, err
	}

	// Handle empty files
	if size == 0 {
		// Empty file handling logic
		// TO DO: Implement API for empty file creation
		return nil, fs.ErrorNotImplemented
	}

	// Execute file upload
	return f.upload(ctx, in, remote, directoryID, size)
}

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Rmdir removes the directory.
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	resp, err := f.getFileList(ctx, dirID, 1, 0)
	if err != nil {
		return err
	}

	if len(resp.Data) > 0 {
		return fs.ErrorDirectoryNotEmpty
	}

	// Delete directory
	_, err = f.deleteFiles(ctx, []string{dirID}, "")
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

// About gets quota information
func (f *Fs) About(ctx context.Context) (usage *fs.Usage, err error) {
	userInfo, err := f.getUserInfo(ctx)
	if err != nil {
		return nil, err
	}
	total, _ := userInfo.Data.RTSpaceInfo.AllTotal.Size.Int64()
	used, _ := userInfo.Data.RTSpaceInfo.AllUse.Size.Int64()
	free, _ := userInfo.Data.RTSpaceInfo.AllRemain.Size.Int64()
	usage = &fs.Usage{
		Total: fs.NewUsageValue(total),
		Used:  fs.NewUsageValue(used),
		Free:  fs.NewUsageValue(free),
	}
	return usage, nil
}

// ---------------------------------------------------------------------------

// ID returns the ID of the object.
func (o *Object) ID() string {
	return o.id
}

// setMetaData sets the metadata from info.
func (o *Object) setMetaData(info *api.FileInfo) error {
	// Ensure it's not a directory
	if info.FC == objectDir {
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
	var nextOffset = 0
	for {
		resp, err := o.fs.getFileList(ctx, directoryID, defaultLimit, nextOffset)
		if err != nil {
			return err
		}
		// Search for matching files in the current page
		for _, item := range resp.Data {
			if item.FN == leaf {
				return o.setMetaData(&item)
			}
		}
		// If the returned count is less than the requested limit, it means we've reached the last page
		if len(resp.Data) < defaultLimit {
			break
		}
		// Update the offset for the next page
		nextOffset += defaultLimit
	}
	return fs.ErrorObjectNotFound
}

// Fs returns the parent Fs.
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// String returns a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects modTime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

// Size returns the file size in bytes
func (o *Object) Size() int64 {
	return o.size
}

// Storable returns true if the object is storable.
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
//
// See Open in the Object interface for documentation.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	// Get download URL
	resp, err := o.fs.getFileDownloadURL(ctx, o.pickCode)
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
	return o.fs.download(ctx, fileInfo.URL.URL, options...)
}

// Update the object with the contents of the io.Reader, modTime and size
//
// If existing is set then it updates the object rather than creating a new one.
//
// The new object may have been created if an error is returned.
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

// Hash returns the SHA-1 of an object returning a lowercase hex string
//
// See Hash in the Object interface for documentation.
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t == hash.SHA1 {
		return o.sha1, nil
	}
	return "", hash.ErrUnsupported
}

// Remove an object
//
// See Remove in the Object interface for documentation.
func (o *Object) Remove(ctx context.Context) error {
	// Delete file
	_, err := o.fs.deleteFiles(ctx, []string{o.id}, "")
	return err
}

// ---------------------------------------------------------------------------

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		return fmt.Errorf("can't move directories across different remotes: %w", fs.ErrorCantMove)
	}

	srcID, srcDirectoryID, _, dstDirectoryID, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}

	// If the destination does not exist, decide whether to rename or move
	if srcDirectoryID == dstDirectoryID {
		// Rename directory within the same parent
		_, err = f.updateFile(ctx, srcID, map[string]string{
			"file_name": dstLeaf,
		})
	} else {
		// Move to a different parent directory
		_, err = f.moveFiles(ctx, []string{srcID}, dstDirectoryID)
	}

	if err != nil {
		return err
	}
	// Flush directory cache
	srcFs.dirCache.FlushDir(srcRemote)
	return nil
}

// Copy src to this remote using server-side copy operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fmt.Errorf("can't copy across different remotes: %w", fs.ErrorCantCopy)
	}

	// Create the destination object and ensure directory exists
	_, _, dstDirID, err := f.createObject(ctx, remote, srcObj.modTime, srcObj.size)
	if err != nil {
		return nil, err
	}

	// Copy file
	_, err = f.copyFiles(ctx, dstDirID, []string{srcObj.id}, false)
	if err != nil {
		return nil, err
	}

	// Flush directory cache
	dstDir, _ := f.getNormalizedPath(remote)
	f.dirCache.FlushDir(dstDir)

	// Return new object
	return f.NewObject(ctx, remote)
}

// Move src to this remote using server-side move operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fmt.Errorf("can't move across different remotes: %w", fs.ErrorCantMove)
	}

	// Create the destination object and ensure directory exists
	_, _, dstDirID, err := f.createObject(ctx, remote, srcObj.modTime, srcObj.size)
	if err != nil {
		return nil, err
	}

	// Move file
	_, err = f.moveFiles(ctx, []string{srcObj.id}, dstDirID)
	if err != nil {
		return nil, err
	}

	// Flush directory cache
	dstDir, _ := f.getNormalizedPath(remote)
	f.dirCache.FlushDir(dstDir)

	// Return new object
	return f.NewObject(ctx, remote)
}

// DirCacheFlush flushes the directory cache - used in testing as an
// optional interface
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// CleanUp cleans up temporary files. Implement this if needed.
func (f *Fs) CleanUp(ctx context.Context) error {
	return nil
}

// OAuth performs the OAuth flow to get a token, implementing config.Configurer.
func (f *Fs) OAuth(ctx context.Context, name string, m configmap.Mapper, oauthConfig *fs.ConfigOut) error {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return err
	}
	appId := opt.AppID
	return f.tokenSource.Auth(appId)
}

// Config handles the configuration process.
func (f *Fs) Config(ctx context.Context, name string, m configmap.Mapper, config fs.ConfigIn) (*fs.ConfigOut, error) {
	switch config.State {
	case "":
		// Check token exists
		if _, err := oauthutil.GetToken(name, m); err != nil {
			return fs.ConfigGoto("choose_auth_type")
		}
		return fs.ConfigConfirm("choose_reauthorize", false, "consent_to_authorize", "Re-authorize for new token?")
	case "choose_reauthorize":
		if config.Result == "false" {
			// User doesn't want to re-authorize, so return empty state
			return nil, nil
		} else {
			// User wants to re-authorize, so proceed to choose auth type
			return fs.ConfigGoto("choose_auth_type")
		}
	case "choose_auth_type":
		return fs.ConfigChooseExclusiveFixed("choose_auth_type_done", "auth_type", "Select authorization type", []fs.OptionExample{
			{Value: "token", Help: "Authenticate using an existing refresh token"},
			{Value: "auth", Help: "Authenticate using your 115 Cloud Open Platform (requires App ID)"},
		})
	case "choose_auth_type_done":
		if config.Result == "auth" {
			return fs.ConfigGoto("authorize")
		} else if config.Result == "token" {
			return fs.ConfigInput("authorize_token", "Enter your refresh token", "Please enter your refresh token")
		}
	case "authorize_token":
		// Use TokenSource to save token
		fc := fshttp.NewClient(ctx)
		ts := &TokenSource{
			c: rest.NewClient(fc),
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
	case "authorize":
		opt := new(Options)
		err := configstruct.Set(m, opt)
		if err != nil {
			return nil, err
		}
		appId := func() string {
			if opt.AppID != "" {
				return opt.AppID
			}
			return defaultAppId
		}()
		// Use TokenSource to save token
		fc := fshttp.NewClient(ctx)
		ts := &TokenSource{
			c:    rest.NewClient(fc),
			ctx:  ctx,
			name: name,
			m:    m,
		}
		err = ts.Auth(appId)
		if err != nil {
			return nil, fmt.Errorf("failed to authenticate: %v", err)
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

// uploadToOSS uploads a file to Alibaba Cloud OSS
func uploadToOSS(ctx context.Context, in io.Reader, initData api.InitUploadData, token api.UploadTokenData) error {
	// Create OSS client
	ossClient, err := oss.New(token.Endpoint, token.AccessKeyId, token.AccessKeySecret, oss.SecurityToken(token.SecurityToken))
	if err != nil {
		return fmt.Errorf("failed to create OSS client: %w", err)
	}

	bucket, err := ossClient.Bucket(initData.Bucket)
	if err != nil {
		return fmt.Errorf("failed to get bucket: %w", err)
	}

	// Parse callback data
	callback, err := initData.GetCallback()
	if err != nil {
		return err
	}

	// Base64 encode callback data
	callbackStr := base64.StdEncoding.EncodeToString([]byte(callback.Callback))
	callbackVarStr := base64.StdEncoding.EncodeToString([]byte(callback.CallbackVar))

	// Perform upload
	err = bucket.PutObject(initData.Object, in,
		oss.Callback(callbackStr),
		oss.CallbackVar(callbackVarStr),
	)
	if err != nil {
		return fmt.Errorf("failed to upload to OSS: %w", err)
	}

	fs.Debugf(nil, "uploaded %s to OSS successfully", initData.Object)
	return nil
}

// uploadMultipartToOSS uploads a file to Alibaba Cloud OSS using multipart upload
// Known limitations:
// Due to some special restrictions on callback by Open115,
// it seems that parallel multipart uploads are not allowed.
func uploadMultipartToOSS(ctx context.Context, in io.Reader, initData api.InitUploadData, token api.UploadTokenData, fileSize, chunkSize int64) error {
	ossClient, err := oss.New(token.Endpoint, token.AccessKeyId, token.AccessKeySecret, oss.SecurityToken(token.SecurityToken))
	if err != nil {
		return fmt.Errorf("failed to create OSS client: %w", err)
	}
	bucket, err := ossClient.Bucket(initData.Bucket)
	if err != nil {
		return fmt.Errorf("failed to get bucket: %w", err)
	}
	callback, err := initData.GetCallback()
	if err != nil {
		return err
	}
	callbackStr := base64.StdEncoding.EncodeToString([]byte(callback.Callback))
	callbackVarStr := base64.StdEncoding.EncodeToString([]byte(callback.CallbackVar))

	imur, err := bucket.InitiateMultipartUpload(initData.Object, oss.Sequential())
	if err != nil {
		return fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	partNum := (fileSize + chunkSize - 1) / chunkSize
	parts := make([]oss.UploadPart, partNum)
	buf := make([]byte, chunkSize)

	for i := int64(1); i <= partNum; i++ {
		if ctx.Err() != nil {
			_ = bucket.AbortMultipartUpload(imur)
			return ctx.Err()
		}
		curSize := chunkSize
		if i == partNum {
			curSize = fileSize - (i-1)*chunkSize
		}
		n, err := io.ReadFull(in, buf[:curSize])
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			_ = bucket.AbortMultipartUpload(imur)
			return fmt.Errorf("failed to read part %d data: %w", i, err)
		}
		part, err := bucket.UploadPart(imur, bytes.NewReader(buf[:n]), int64(n), int(i))
		if err != nil {
			_ = bucket.AbortMultipartUpload(imur)
			return fmt.Errorf("failed to upload part %d: %w", i, err)
		}
		parts[i-1] = part
	}

	_, err = bucket.CompleteMultipartUpload(
		imur,
		parts,
		oss.Callback(callbackStr),
		oss.CallbackVar(callbackVarStr),
	)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}
	fs.Debugf(nil, "multipart uploaded %s to OSS successfully", initData.Object)
	return nil
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
func (f *Fs) initializeUpload(ctx context.Context, remote, directoryID string, size int64, fileSHA1 string, reader io.ReadSeeker) (*api.InitUploadData, error) {
	// Build upload initialization request
	initReq := &api.InitUploadRequest{
		FileName: path.Base(remote),
		FileSize: size,
		Target:   "U_1_" + directoryID, // Format: U_1_dirID
		FileID:   fileSHA1,
	}

	// Execute upload initialization request
	initResp, err := f.initUpload(ctx, initReq)
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
		initResp, err = f.initUpload(ctx, initReq)
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
func (f *Fs) upload(ctx context.Context, in io.Reader, remote string,
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
	initData, err := f.initializeUpload(ctx, remote, directoryID, size, fileSHA1, reader)
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
	tokenResp, err := f.getUploadToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get upload token: %w", err)
	}

	// Reset file position for upload
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	// Calculate chunk size
	chunkSize := calPartSize(size)

	// Choose upload method based on file size
	if chunkSize >= size {
		// Use single upload for small files
		err = uploadToOSS(ctx, reader, *initData, tokenResp.Data)
	} else {
		// Use multipart upload for large files
		err = uploadMultipartToOSS(ctx, reader, *initData, tokenResp.Data, size, chunkSize)
	}
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

func shouldRetry(ctx context.Context, res *http.Response, resp *api.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if resp != nil && resp.Errno != 0 {
		return false, fserrors.NoRetryError(fmt.Errorf("API error: code=%d, message=%s", resp.Errno, resp.Error))
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(res, retryErrorCodes), err
}

// download starts a download from the given URL and returns the response body reader
func (f *Fs) download(ctx context.Context, url string, options ...fs.OpenOption) (io.ReadCloser, error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: url,
	}
	opts.Options = options
	var resp *http.Response
	err := f.pacer.Call(func() (bool, error) {
		var err error
		resp, err = f.client.Call(ctx, &opts)
		return shouldRetry(ctx, resp, nil, err)
	})
	return resp.Body, err
}

// createFolder creates a new folder.
func (f *Fs) createFolder(ctx context.Context, pid string, fileName string) (*api.FolderCreateResponse, error) {
	values := url.Values{}
	values.Set("pid", pid)
	values.Set("file_name", fileName)
	opts := rest.Opts{
		Method:      "POST",
		RootURL:     baseAPI,
		Path:        "/open/folder/add",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(values.Encode()),
	}
	var resp api.FolderCreateResponse
	err := f.pacer.Call(func() (bool, error) {
		r, err := f.client.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// getFileList gets the list of files and folders.
func (f *Fs) getFileList(ctx context.Context, cid string, limit, offset int) (*api.FileListResponse, error) {

	// Build query parameters
	params := url.Values{}
	params.Set("cid", cid)
	params.Set("limit", fmt.Sprintf("%d", limit))
	params.Set("offset", fmt.Sprintf("%d", offset))
	params.Set("cur", "1")
	params.Set("stdir", "1")
	params.Set("show_dir", "1")
	opts := rest.Opts{
		Method:     "GET",
		RootURL:    baseAPI,
		Path:       "/open/ufile/files",
		Parameters: params,
	}
	var resp api.FileListResponse
	err := f.pacer.Call(func() (bool, error) {
		r, err := f.client.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// getFileInfo gets the details of a file or folder.
func (f *Fs) getFileInfo(ctx context.Context, fileID string) (*api.FileInfoResponse, error) {
	opts := rest.Opts{
		Method:     "GET",
		RootURL:    baseAPI,
		Path:       "/open/folder/get_info",
		Parameters: url.Values{"file_id": []string{fileID}},
	}

	var resp api.FileInfoResponse
	err := f.pacer.Call(func() (bool, error) {
		r, err := f.client.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// getFileDownloadURL gets the download URL for a file.
func (f *Fs) getFileDownloadURL(ctx context.Context, pickCode string) (*api.FileDownloadResponse, error) {
	opts := rest.Opts{
		Method:      "POST",
		RootURL:     baseAPI,
		Path:        "/open/ufile/downurl",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(fmt.Sprintf("pick_code=%s", pickCode)),
	}

	var resp api.FileDownloadResponse
	err := f.pacer.Call(func() (bool, error) {
		r, err := f.client.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// deleteFiles deletes files or folders.
func (f *Fs) deleteFiles(ctx context.Context, fileIDs []string, parentID string) (*api.FileOperationResponse, error) {
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
	}

	var resp api.FileOperationResponse
	err := f.pacer.Call(func() (bool, error) {
		r, err := f.client.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// updateFile updates file information (rename or star).
func (f *Fs) updateFile(ctx context.Context, fileID string, options map[string]string) (*api.FileUpdateResponse, error) {
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
	}

	var resp api.FileUpdateResponse
	err := f.pacer.Call(func() (bool, error) {
		r, err := f.client.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// copyFiles copies files.
func (f *Fs) copyFiles(ctx context.Context, pid string, fileIDs []string, noDuplicate bool) (*api.FileOperationResponse, error) {
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
	}

	var resp api.FileOperationResponse
	err := f.pacer.Call(func() (bool, error) {
		r, err := f.client.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// moveFiles moves files.
func (f *Fs) moveFiles(ctx context.Context, fileIDs []string, toCID string) (*api.FileOperationResponse, error) {
	formData := fmt.Sprintf("file_ids=%s&to_cid=%s", strings.Join(fileIDs, ","), toCID)

	opts := rest.Opts{
		Method:      "POST",
		RootURL:     baseAPI,
		Path:        "/open/ufile/move",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(formData),
	}

	var resp api.FileOperationResponse
	err := f.pacer.Call(func() (bool, error) {
		r, err := f.client.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// getUploadToken gets the upload token
func (f *Fs) getUploadToken(ctx context.Context) (*api.UploadTokenResponse, error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: baseAPI,
		Path:    "/open/upload/get_token",
	}

	var resp api.UploadTokenResponse
	err := f.pacer.Call(func() (bool, error) {
		r, err := f.client.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// initUpload initializes file upload
func (f *Fs) initUpload(ctx context.Context, req *api.InitUploadRequest) (*api.InitUploadResponse, error) {
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
	}

	var resp api.InitUploadResponse
	err := f.pacer.Call(func() (bool, error) {
		r, err := f.client.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// resumeUpload handles resumable upload
func (f *Fs) resumeUpload(ctx context.Context, req *api.ResumeUploadRequest) (*api.ResumeUploadResponse, error) {
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
	}

	var resp api.ResumeUploadResponse
	err := f.pacer.Call(func() (bool, error) {
		r, err := f.client.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// getUserInfo gets the user information including space usage and VIP status.
func (f *Fs) getUserInfo(ctx context.Context) (*api.UserInfoResponse, error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: baseAPI,
		Path:    "/open/user/info",
	}

	var resp api.UserInfoResponse
	err := f.pacer.Call(func() (bool, error) {
		r, err := f.client.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetry(ctx, r, &resp.Response, err)
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// getNormalizedPath splits a path into a parent and a leaf.
// The parent is normalized to an empty string if it is "." or "/".
func (f *Fs) getNormalizedPath(p string) (parent, leaf string) {
	parent = path.Dir(p)
	if parent == "." || parent == "/" {
		parent = ""
	}
	leaf = path.Base(p)
	return
}

// Interfaces implementation check
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ dircache.DirCacher = (*Fs)(nil)
)
