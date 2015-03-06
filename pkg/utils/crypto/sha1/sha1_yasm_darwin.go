// !build amd64

package sha1

//go:generate yasm -f macho64 -DINTEL_SHA1_UPDATE_FUNCNAME=_sha1_update_intel sha1_sse3_amd64.asm -o sha1_sse3_amd64.syso
