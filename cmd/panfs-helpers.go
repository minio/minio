package cmd

import (
	"context"
	"os"
	"strings"

	"github.com/minio/minio/internal/logger"
)

// removePanFSBucketDir removes panfs bucket only if it is empty. We consider the bucket as empty if it is contains no
// files and other directories or contains only .s3 directory which is hidden from the user
func removePanFSBucketDir(ctx context.Context, dirPath string) (err error) {
	if dirPath == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		logger.LogIf(ctx, errInvalidArgument)
		return err
	}

	f, err := os.Open(dirPath)
	if err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		}
		return err
	}
	entries, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return err
	}

	if len(entries) > 1 {
		// Only single entry (.s3 directory) allowed to be inside bucket dir to consider bucket as empty
		return errVolumeNotEmpty
	}
	if len(entries) != 0 && entries[0] != panfsMetaDir {
		return errVolumeNotEmpty
	}
	if err = removeAll(dirPath); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		}
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

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
