// !build amd64

package erasure

//go:generate yasm -f elf64 ec_multibinary.asm -o ec_multibinary.syso
//go:generate yasm -f elf64 gf_2vect_mad_avx2.asm -o gf_2vect_mad_avx2.syso
//go:generate yasm -f elf64 gf_2vect_mad_avx.asm -o gf_2vect_mad_avx.syso
//go:generate yasm -f elf64 gf_2vect_mad_sse.asm -o gf_2vect_mad_sse.syso
//go:generate yasm -f elf64 gf_3vect_mad_avx2.asm -o gf_3vect_mad_avx2.syso
//go:generate yasm -f elf64 gf_3vect_mad_avx.asm -o gf_3vect_mad_avx.syso
//go:generate yasm -f elf64 gf_3vect_mad_sse.asm -o gf_3vect_mad_sse.syso
//go:generate yasm -f elf64 gf_4vect_mad_avx2.asm -o gf_4vect_mad_avx2.syso
//go:generate yasm -f elf64 gf_4vect_mad_avx.asm -o gf_4vect_mad_avx.syso
//go:generate yasm -f elf64 gf_4vect_mad_sse.asm -o gf_4vect_mad_sse.syso
//go:generate yasm -f elf64 gf_5vect_mad_avx2.asm -o gf_5vect_mad_avx2.syso
//go:generate yasm -f elf64 gf_5vect_mad_avx.asm -o gf_5vect_mad_avx.syso
//go:generate yasm -f elf64 gf_5vect_mad_sse.asm -o gf_5vect_mad_sse.syso
//go:generate yasm -f elf64 gf_6vect_mad_avx2.asm -o gf_6vect_mad_avx2.syso
//go:generate yasm -f elf64 gf_6vect_mad_avx.asm -o gf_6vect_mad_avx.syso
//go:generate yasm -f elf64 gf_6vect_mad_sse.asm -o gf_6vect_mad_sse.syso
//go:generate yasm -f elf64 gf_vect_mad_avx2.asm -o gf_vect_mad_avx2.syso
//go:generate yasm -f elf64 gf_vect_mad_avx.asm -o gf_vect_mad_avx.syso
//go:generate yasm -f elf64 gf_vect_mad_sse.asm -o gf_vect_mad_sse.syso
//go:generate yasm -f elf64 gf_2vect_dot_prod_avx2.asm -o gf_2vect_dot_prod_avx2.syso
//go:generate yasm -f elf64 gf_2vect_dot_prod_avx.asm -o gf_2vect_dot_prod_avx.syso
//go:generate yasm -f elf64 gf_2vect_dot_prod_sse.asm -o gf_2vect_dot_prod_sse.syso
//go:generate yasm -f elf64 gf_3vect_dot_prod_avx2.asm -o gf_3vect_dot_prod_avx2.syso
//go:generate yasm -f elf64 gf_3vect_dot_prod_avx.asm -o gf_3vect_dot_prod_avx.syso
//go:generate yasm -f elf64 gf_3vect_dot_prod_sse.asm -o gf_3vect_dot_prod_sse.syso
//go:generate yasm -f elf64 gf_4vect_dot_prod_avx2.asm -o gf_4vect_dot_prod_avx2.syso
//go:generate yasm -f elf64 gf_4vect_dot_prod_avx.asm -o gf_4vect_dot_prod_avx.syso
//go:generate yasm -f elf64 gf_4vect_dot_prod_sse.asm -o gf_4vect_dot_prod_sse.syso
//go:generate yasm -f elf64 gf_5vect_dot_prod_avx2.asm -o gf_5vect_dot_prod_avx2.syso
//go:generate yasm -f elf64 gf_5vect_dot_prod_avx.asm -o gf_5vect_dot_prod_avx.syso
//go:generate yasm -f elf64 gf_5vect_dot_prod_sse.asm -o gf_5vect_dot_prod_sse.syso
//go:generate yasm -f elf64 gf_6vect_dot_prod_avx2.asm -o gf_6vect_dot_prod_avx2.syso
//go:generate yasm -f elf64 gf_6vect_dot_prod_avx.asm -o gf_6vect_dot_prod_avx.syso
//go:generate yasm -f elf64 gf_6vect_dot_prod_sse.asm -o gf_6vect_dot_prod_sse.syso
//go:generate yasm -f elf64 gf_vect_dot_prod_avx2.asm -o gf_vect_dot_prod_avx2.syso
//go:generate yasm -f elf64 gf_vect_dot_prod_avx.asm -o gf_vect_dot_prod_avx.syso
//go:generate yasm -f elf64 gf_vect_dot_prod_sse.asm -o gf_vect_dot_prod_sse.syso
//go:generate yasm -f elf64 gf_vect_mul_avx.asm -o gf_vect_mul_avx.syso
//go:generate yasm -f elf64 gf_vect_mul_sse.asm -o gf_vect_mul_sse.syso
