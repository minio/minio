package cmd

import (
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
