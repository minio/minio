package cmd

import (
	"fmt"
	"os"
	"path"
	"strings"
)

// dotS3PrefixCheck validates object (bucket) names to be compliant with the internal structure of the panfs s3 backend
// Returns an error whether name equals to .s3 or has .s3 prefix
func dotS3PrefixCheck(objects ...string) error {
	for _, item := range objects {
		if item == panfsMetaDir || strings.HasPrefix(item, panfsMetaDir+SlashSeparator) {
			return PanFSS3InvalidName{}
		}
	}
	return nil
}

// Publish source path to destination path, creates all the
// missing parents if they don't exist and change the rights.
func panfsPublishFile(srcFilePath, dstFilePath string, objMode, dirMode os.FileMode, ownerID, groupID int) (err error) {
	if srcFilePath == "" || dstFilePath == "" {
		return errInvalidArgument
	}

	if err = checkPathLength(srcFilePath); err != nil {
		return err
	}
	if err = checkPathLength(dstFilePath); err != nil {
		return err
	}

	if err = panOSMkdirAll(path.Dir(dstFilePath), dirMode, ownerID, groupID); err != nil {
		return prepareErrorPublishFile("", dstFilePath, err)
	}

	if err = os.Chmod(srcFilePath, objMode); err != nil {
		return prepareErrorPublishFile(srcFilePath, "", err)
	}
	if err = os.Chown(srcFilePath, ownerID, groupID); err != nil {
		return prepareErrorPublishFile(srcFilePath, "", err)
	}

	if err = PanRenameFile(srcFilePath, dstFilePath); err != nil {
		return prepareErrorPublishFile(srcFilePath, dstFilePath, err)
	}
	return nil
}

func prepareErrorPublishFile(srcFilePath, dstFilePath string, err error) error {
	switch {
	case isSysErrNotDir(err) && !osIsNotExist(err):
		// Windows can have both isSysErrNotDir(err) and osIsNotExist(err) returning
		// true if the source file path contains an non-existent directory. In that case,
		// we want to return errFileNotFound instead, which will honored in subsequent
		// switch cases
		return errFileAccessDenied
	case isSysErrPathNotFound(err):
		// This is a special case should be handled only for
		// windows, because windows API does not return "not a
		// directory" error message. Handle this specifically here.
		return errFileAccessDenied
	case isSysErrCrossDevice(err):
		return fmt.Errorf("%w (%s)->(%s)", errCrossDeviceLink, srcFilePath, dstFilePath)
	case osIsNotExist(err):
		return errFileNotFound
	case osIsExist(err):
		// This is returned only when destination is a directory and we
		// are attempting a rename from file to directory.
		return errIsNotRegular
	default:
		return err
	}
}

// PanRenameFile captures time taken to call Link/Unlink
func PanRenameFile(src, dst string) error {
	defer updateOSMetrics(osMetricRename, src, dst)()
	if err := os.Link(src, dst); err != nil {
		if !os.IsExist(err) {
			return err
		}
		// dst file exists
		if err = os.Remove(dst); err != nil {
			return err
		}
		if err = os.Link(src, dst); err != nil {
			return err
		}
	}
	return os.Remove(src)
}

func panOSMkdirAll(dirPath string, perm os.FileMode, ownerID, groupID int) error {
	// Fast path: if we can tell whether path is a directory or file, stop with success or error.
	err := Access(dirPath)
	if err == nil {
		return nil
	}
	if !osIsNotExist(err) {
		return &os.PathError{Op: "mkdir", Path: dirPath, Err: err}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(dirPath)
	for i > 0 && os.IsPathSeparator(dirPath[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(dirPath[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent.
		if err = panOSMkdirAll(dirPath[:j-1], perm, ownerID, groupID); err != nil {
			return err
		}
	}

	// Parent now exists; invoke Mkdir and use its result.
	if err = Mkdir(dirPath, perm); err != nil && !osIsExist(err) {
		return err
	}
	if err = os.Chown(dirPath, ownerID, groupID); err != nil {
		return err
	}

	return nil
}
