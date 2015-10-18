package disk

// StatFS stat fs struct is container which holds following values
// Total - total size of the volume / disk
// Free - free size of the volume / disk
// FSType - file system type string
type StatFS struct {
	Total  int64
	Free   int64
	FSType string
}
