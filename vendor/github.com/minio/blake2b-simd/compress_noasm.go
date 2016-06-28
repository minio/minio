//+build !amd64 noasm appengine

// Written in 2012 by Dmitry Chestnykh.
//
// To the extent possible under law, the author have dedicated all copyright
// and related and neighboring rights to this software to the public domain
// worldwide. This software is distributed without any warranty.
// http://creativecommons.org/publicdomain/zero/1.0/

package blake2b

func compress(d *digest, p []uint8) {
	compressGeneric(d, p)
}
