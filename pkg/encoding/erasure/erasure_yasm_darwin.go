// !build amd64

package erasure

//go:generate yasm -f macho64 ec-multibinary.asm -o ec-multibinary.syso
//go:generate yasm -f macho64 gf-2vect-dot-prod-avx2.asm -o gf-2vect-dot-prod-avx2.syso
//go:generate yasm -f macho64 gf-2vect-dot-prod-avx.asm -o gf-2vect-dot-prod-avx.syso
//go:generate yasm -f macho64 gf-2vect-dot-prod-sse.asm -o gf-2vect-dot-prod-sse.syso
//go:generate yasm -f macho64 gf-3vect-dot-prod-avx2.asm -o gf-3vect-dot-prod-avx2.syso
//go:generate yasm -f macho64 gf-3vect-dot-prod-avx.asm -o gf-3vect-dot-prod-avx.syso
//go:generate yasm -f macho64 gf-3vect-dot-prod-sse.asm -o gf-3vect-dot-prod-sse.syso
//go:generate yasm -f macho64 gf-4vect-dot-prod-avx2.asm -o gf-4vect-dot-prod-avx2.syso
//go:generate yasm -f macho64 gf-4vect-dot-prod-avx.asm -o gf-4vect-dot-prod-avx.syso
//go:generate yasm -f macho64 gf-4vect-dot-prod-sse.asm -o gf-4vect-dot-prod-sse.syso
//go:generate yasm -f macho64 gf-5vect-dot-prod-avx2.asm -o gf-5vect-dot-prod-avx2.syso
//go:generate yasm -f macho64 gf-5vect-dot-prod-avx.asm -o gf-5vect-dot-prod-avx.syso
//go:generate yasm -f macho64 gf-5vect-dot-prod-sse.asm -o gf-5vect-dot-prod-sse.syso
//go:generate yasm -f macho64 gf-6vect-dot-prod-avx2.asm -o gf-6vect-dot-prod-avx2.syso
//go:generate yasm -f macho64 gf-6vect-dot-prod-avx.asm -o gf-6vect-dot-prod-avx.syso
//go:generate yasm -f macho64 gf-6vect-dot-prod-sse.asm -o gf-6vect-dot-prod-sse.syso
//go:generate yasm -f macho64 gf-vect-dot-prod-avx2.asm -o gf-vect-dot-prod-avx2.syso
//go:generate yasm -f macho64 gf-vect-dot-prod-avx.asm -o gf-vect-dot-prod-avx.syso
//go:generate yasm -f macho64 gf-vect-dot-prod-sse.asm -o gf-vect-dot-prod-sse.syso
//go:generate yasm -f macho64 gf-vect-mul-avx.asm -o gf-vect-mul-avx.syso
//go:generate yasm -f macho64 gf-vect-mul-sse.asm -o gf-vect-mul-sse.syso
