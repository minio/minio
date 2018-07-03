//+build appengine

package gjson

func getBytes(json []byte, path string) Result {
	return Get(string(json), path)
}
func fillIndex(json string, c *parseContext) {
	// noop. Use zero for the Index value.
}
