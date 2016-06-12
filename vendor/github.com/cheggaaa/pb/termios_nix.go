// +build linux solaris
// +build !appengine

package pb

const ioctlReadTermios = 0x5401  // syscall.TCGETS
const ioctlWriteTermios = 0x5402 // syscall.TCSETS
