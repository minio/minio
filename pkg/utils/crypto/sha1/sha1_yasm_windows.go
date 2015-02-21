// !build amd64

package sha1

//go:generate yasm -f win64 -DWIN_ABI=1 sha1_sse3_amd64.asm -o sha1_sse3_amd64.syso
