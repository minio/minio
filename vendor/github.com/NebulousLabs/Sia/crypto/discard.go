package crypto

// SecureWipe destroys the data contained within a byte slice. There are no
// strong guarantees that all copies of the memory have been eliminated. If the
// OS was doing context switching or using swap space the keys may still be
// elsewhere in memory.
func SecureWipe(data []byte) {
	for i := range data {
		data[i] = 0
	}
}
