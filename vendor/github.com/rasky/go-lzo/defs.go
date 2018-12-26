package lzo

const (
	m1_MAX_OFFSET = 0x0400
	m2_MAX_OFFSET = 0x0800
	m3_MAX_OFFSET = 0x4000
	m4_MAX_OFFSET = 0xbfff
	mX_MAX_OFFSET = m1_MAX_OFFSET + m2_MAX_OFFSET

	m1_MIN_LEN = 2
	m1_MAX_LEN = 2
	m2_MIN_LEN = 3
	m2_MAX_LEN = 8
	m3_MIN_LEN = 3
	m3_MAX_LEN = 33
	m4_MIN_LEN = 3
	m4_MAX_LEN = 9

	m1_MARKER = 0
	m2_MARKER = 64
	m3_MARKER = 32
	m4_MARKER = 16
)

const (
	d_BITS = 14
	d_MASK = (1 << d_BITS) - 1
	d_HIGH = (d_MASK >> 1) + 1
)
