package useragent

var provider = func() string {
	return "Go-http-client/1.1"
}

// Register sets a custom function to provide the User-Agent string.
func Register(f func() string) {
	provider = f
}

// Get returns the current User-Agent string.
func Get() string {
	return provider()
}
