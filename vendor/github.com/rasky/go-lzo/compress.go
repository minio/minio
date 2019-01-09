package lzo

func appendMulti(out []byte, t int) []byte {
	for t > 255 {
		out = append(out, 0)
		t -= 255
	}
	out = append(out, byte(t))
	return out
}

func compress(in []byte) (out []byte, sz int) {
	var m_off int
	in_len := len(in)
	ip_len := in_len - m2_MAX_LEN - 5
	dict := make([]int32, 1<<d_BITS)
	ii := 0
	ip := 4
	for {
		key := int(in[ip+3])
		key = (key << 6) ^ int(in[ip+2])
		key = (key << 5) ^ int(in[ip+1])
		key = (key << 5) ^ int(in[ip+0])
		dindex := ((0x21 * key) >> 5) & d_MASK
		m_pos := int(dict[dindex]) - 1
		if m_pos < 0 {
			goto literal
		}
		if ip == m_pos || (ip-m_pos) > m4_MAX_OFFSET {
			goto literal
		}
		m_off = ip - m_pos
		if m_off <= m2_MAX_OFFSET || in[m_pos+3] == in[ip+3] {
			goto try_match
		}

		dindex = (dindex & (d_MASK & 0x7ff)) ^ (d_HIGH | 0x1f)
		m_pos = int(dict[dindex]) - 1
		if m_pos < 0 {
			goto literal
		}
		if ip == m_pos || (ip-m_pos) > m4_MAX_OFFSET {
			goto literal
		}
		m_off = ip - m_pos
		if m_off <= m2_MAX_OFFSET || in[m_pos+3] == in[ip+3] {
			goto try_match
		}

		goto literal

	try_match:
		if in[m_pos] == in[ip] && in[m_pos+1] == in[ip+1] && in[m_pos+2] == in[ip+2] {
			goto match
		}

	literal:
		dict[dindex] = int32(ip + 1)
		ip += 1 + (ip-ii)>>5
		if ip >= ip_len {
			break
		}
		continue

	match:
		dict[dindex] = int32(ip + 1)
		if ip != ii {
			t := ip - ii
			if t <= 3 {
				out[len(out)-2] |= byte(t)
			} else if t <= 18 {
				out = append(out, byte(t-3))
			} else {
				out = append(out, 0)
				out = appendMulti(out, t-18)
			}

			out = append(out, in[ii:ii+t]...)
			ii += t
		}

		var i int
		ip += 3
		for i = 3; i < 9; i++ {
			ip++
			if in[m_pos+i] != in[ip-1] {
				break
			}
		}
		if i < 9 {
			ip--
			m_len := ip - ii
			if m_off <= m2_MAX_OFFSET {
				m_off -= 1
				out = append(out,
					byte((((m_len - 1) << 5) | ((m_off & 7) << 2))),
					byte((m_off >> 3)))
			} else if m_off <= m3_MAX_OFFSET {
				m_off -= 1
				out = append(out,
					byte(m3_MARKER|(m_len-2)),
					byte((m_off&63)<<2),
					byte(m_off>>6))
			} else {
				m_off -= 0x4000
				out = append(out,
					byte(m4_MARKER|((m_off&0x4000)>>11)|(m_len-2)),
					byte((m_off&63)<<2),
					byte(m_off>>6))
			}
		} else {
			m := m_pos + m2_MAX_LEN + 1
			for ip < in_len && in[m] == in[ip] {
				m++
				ip++
			}
			m_len := ip - ii
			if m_off <= m3_MAX_OFFSET {
				m_off -= 1
				if m_len <= 33 {
					out = append(out, byte(m3_MARKER|(m_len-2)))
				} else {
					m_len -= 33
					out = append(out, byte(m3_MARKER|0))
					out = appendMulti(out, m_len)
				}
			} else {
				m_off -= 0x4000
				if m_len <= m4_MAX_LEN {
					out = append(out, byte(m4_MARKER|((m_off&0x4000)>>11)|(m_len-2)))
				} else {
					m_len -= m4_MAX_LEN
					out = append(out, byte(m4_MARKER|((m_off&0x4000)>>11)))
					out = appendMulti(out, m_len)
				}
			}
			out = append(out, byte((m_off&63)<<2), byte(m_off>>6))
		}

		ii = ip
		if ip >= ip_len {
			break
		}
	}

	sz = in_len - ii
	return
}

// Compress an input buffer with LZO1X
func Compress1X(in []byte) (out []byte) {
	var t int

	in_len := len(in)
	if in_len <= m2_MAX_LEN+5 {
		t = in_len
	} else {
		out, t = compress(in)
	}

	if t > 0 {
		ii := in_len - t
		if len(out) == 0 && t <= 238 {
			out = append(out, byte(17+t))
		} else if t <= 3 {
			out[len(out)-2] |= byte(t)
		} else if t <= 18 {
			out = append(out, byte(t-3))
		} else {
			out = append(out, 0)
			out = appendMulti(out, t-18)
		}
		out = append(out, in[ii:ii+t]...)
	}

	out = append(out, m4_MARKER|1, 0, 0)
	return
}
