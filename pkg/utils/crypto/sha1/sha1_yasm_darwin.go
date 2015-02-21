// !build amd64

package sha1

//go:generate yasm -f macho64 sha1_sse3_amd64.asm -o sha1_sse3_amd64.syso
