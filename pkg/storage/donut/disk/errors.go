package disk

// InvalidArgument invalid argument
type InvalidArgument struct{}

func (e InvalidArgument) Error() string {
	return "Invalid argument"
}

// UnsupportedFilesystem unsupported filesystem type
type UnsupportedFilesystem struct {
	Type string
}

func (e UnsupportedFilesystem) Error() string {
	return "Unsupported filesystem: " + e.Type
}
