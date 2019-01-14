package lzo

const (
	cSWD_N         = m4_MAX_OFFSET  // ring buffer size
	cSWD_THRESHOLD = 1              // lower limit for match length
	cSWD_F         = 2048           // upper limit for match length
	cSWD_BEST_OFF  = m3_MAX_LEN + 1 // max(m2,m3,m4)+1
	cSWD_HSIZE     = 16384
	cSWD_MAX_CHAIN = 2048
)

type swd struct {
	// Public builtin
	SwdN         uint
	SwdF         uint
	SwdThreshold uint

	// Public configuration
	MaxChain   uint
	NiceLength uint
	UseBestOff bool
	LazyInsert uint

	// Output
	MLen    uint
	MOff    uint
	Look    uint
	BChar   int
	BestOff [cSWD_BEST_OFF]uint

	// Semi-public
	ctx     *compressor
	mpos    uint
	bestPos [cSWD_BEST_OFF]uint

	// Private
	ip        uint // input pointer (lookahead)
	bp        uint // buffer pointer
	rp        uint // remove pointer
	bsize     uint
	bwrap     []byte
	nodecount uint
	firstrp   uint

	b     [cSWD_N + cSWD_F + cSWD_F]byte
	head3 [cSWD_HSIZE]uint16
	succ3 [cSWD_N + cSWD_F]uint16
	best3 [cSWD_N + cSWD_F]uint16
	llen3 [cSWD_HSIZE]uint16
	head2 [65536]uint16
}

func head2(data []byte) uint {
	return uint(data[1])<<8 | uint(data[0])
}

func head3(data []byte) uint {
	key := uint(data[0])
	key = (key << 5) ^ uint(data[1])
	key = (key << 5) ^ uint(data[2])
	key = (key * 0x9f5f) >> 5
	return key & (cSWD_HSIZE - 1)
}

func (s *swd) gethead3(key uint) uint16 {
	if s.llen3[key] == 0 {
		return 0xFFFF
	}
	return s.head3[key]
}

func (s *swd) removeNode(node uint) {
	if s.nodecount == 0 {
		key := head3(s.b[node:])
		if s.llen3[key] == 0 {
			panic("assert: swd.removeNode: invalid llen3")
		}
		s.llen3[key]--

		key = head2(s.b[node:])
		if s.head2[key] == 0xFFFF {
			panic("assert: swd.removeNode: invalid head2")
		}
		if uint(s.head2[key]) == node {
			s.head2[key] = 0xFFFF
		}
		return
	}
	s.nodecount--
}

func (s *swd) init() {
	s.SwdN = cSWD_N
	s.SwdF = cSWD_F
	s.SwdThreshold = cSWD_THRESHOLD

	s.MaxChain = cSWD_MAX_CHAIN
	s.NiceLength = s.SwdF
	s.bsize = s.SwdN + s.SwdF
	s.bwrap = s.b[s.bsize:]
	s.nodecount = s.SwdN

	for i := 0; i < len(s.head2); i++ {
		s.head2[i] = 0xFFFF
	}

	s.ip = 0
	s.bp = s.ip
	s.firstrp = s.ip
	if s.ip+s.SwdF > s.bsize {
		panic("assert: swd.init: invalid ip")
	}

	s.Look = uint(len(s.ctx.in)) - s.ip
	if s.Look > 0 {
		if s.Look > s.SwdF {
			s.Look = s.SwdF
		}
		copy(s.b[s.ip:], s.ctx.in[:s.Look])
		s.ctx.ip += int(s.Look)
		s.ip += s.Look
	}

	if s.ip == s.bsize {
		s.ip = 0
	}

	s.rp = s.firstrp
	if s.rp >= s.nodecount {
		s.rp -= s.nodecount
	} else {
		s.rp += s.bsize - s.nodecount
	}

	if s.Look < 3 {
		s.b[s.bp+s.Look] = 0
		s.b[s.bp+s.Look+1] = 0
		s.b[s.bp+s.Look+2] = 0
	}
}

func (s *swd) getbyte() {
	c := -1
	if s.ctx.ip < len(s.ctx.in) {
		c = int(s.ctx.in[s.ctx.ip])
		s.ctx.ip++
		s.b[s.ip] = byte(c)
		if s.ip < s.SwdF {
			s.bwrap[s.ip] = byte(c)
		}
	} else {
		if s.Look > 0 {
			s.Look--
		}
		s.b[s.ip] = 0
		if s.ip < s.SwdF {
			s.bwrap[s.ip] = 0
		}
	}

	s.ip++
	if s.ip == s.bsize {
		s.ip = 0
	}
	s.bp++
	if s.bp == s.bsize {
		s.bp = 0
	}
	s.rp++
	if s.rp == s.bsize {
		s.rp = 0
	}
}

func (s *swd) accept(n uint) {
	if n > s.Look {
		panic("swd: accept: invalid n")
	}

	for i := uint(0); i < n; i++ {
		s.removeNode(s.rp)

		key := head3(s.b[s.bp:])
		s.succ3[s.bp] = s.gethead3(key)
		s.head3[key] = uint16(s.bp)
		s.best3[s.bp] = uint16(s.SwdF + 1)
		s.llen3[key]++
		if uint(s.llen3[key]) > s.SwdN {
			panic("swd: accept: invalid llen3")
		}

		key = head2(s.b[s.bp:])
		s.head2[key] = uint16(s.bp)

		s.getbyte()
	}
}

func (s *swd) search(node uint, cnt uint) {
	if s.MLen <= 0 {
		panic("assert: search: invalid mlen")
	}

	mlen := s.MLen
	bp := s.bp
	bx := s.bp + s.Look

	scanend1 := s.b[s.bp+mlen-1]
	for ; cnt > 0; cnt-- {
		p1 := bp
		p2 := node
		px := bx

		if mlen >= s.Look {
			panic("assert: search: invalid mlen in loop")
		}
		if s.b[p2+mlen-1] == scanend1 &&
			s.b[p2+mlen] == s.b[p1+mlen] &&
			s.b[p2] == s.b[p1] &&
			s.b[p2+1] == s.b[p1+1] {

			if s.b[bp] != s.b[node] || s.b[bp+1] != s.b[node+1] || s.b[bp+2] != s.b[node+2] {
				panic("assert: seach: invalid initial match")
			}
			p1 = p1 + 2
			p2 = p2 + 2
			for p1 < px {
				p1++
				p2++
				if s.b[p1] != s.b[p2] {
					break
				}
			}
			i := p1 - bp

			for j := uint(0); j < i; j++ {
				if s.b[s.bp+j] != s.b[node+j] {
					panic("assert: search: invalid final match")
				}
			}

			if i < cSWD_BEST_OFF {
				if s.bestPos[i] == 0 {
					s.bestPos[i] = node + 1
				}
			}
			if i > mlen {
				mlen = i
				s.MLen = mlen
				s.mpos = node
				if mlen == s.Look {
					return
				}
				if mlen >= s.NiceLength {
					return
				}
				if mlen > uint(s.best3[node]) {
					return
				}
				scanend1 = s.b[s.bp+mlen-1]
			}
		}

		node = uint(s.succ3[node])
	}
}

func (s *swd) search2() bool {
	if s.Look < 2 {
		panic("assert: search2: invalid look")
	}
	if s.MLen <= 0 {
		panic("assert: search2: invalid mlen")
	}

	key := s.head2[head2(s.b[s.bp:])]
	if key == 0xFFFF {
		return false
	}
	if s.b[s.bp] != s.b[key] || s.b[s.bp+1] != s.b[key+1] {
		panic("assert: search2: invalid key found")
	}
	if s.bestPos[2] == 0 {
		s.bestPos[2] = uint(key + 1)
	}
	if s.MLen < 2 {
		s.MLen = 2
		s.mpos = uint(key)
	}
	return true
}

func (s *swd) findbest() {
	if s.MLen == 0 {
		panic("swd: findbest: invalid mlen")
	}

	key := head3(s.b[s.bp:])
	node := s.gethead3(key)
	s.succ3[s.bp] = node
	cnt := uint(s.llen3[key])
	s.llen3[key]++
	if cnt > s.SwdN+s.SwdF {
		panic("swd: findbest: invalid llen3")
	}
	if cnt > s.MaxChain && s.MaxChain > 0 {
		cnt = s.MaxChain
	}
	s.head3[key] = uint16(s.bp)

	s.BChar = int(s.b[s.bp])
	len := s.MLen
	if s.MLen >= s.Look {
		if s.Look == 0 {
			s.BChar = -1
		}
		s.MOff = 0
		s.best3[s.bp] = uint16(s.SwdF + 1)
	} else {
		if s.search2() && s.Look >= 3 {
			s.search(uint(node), cnt)
		}

		if s.MLen > len {
			s.MOff = s.pos2off(s.mpos)
		}

		if s.UseBestOff {
			for i := 2; i < cSWD_BEST_OFF; i++ {
				if s.bestPos[i] > 0 {
					s.BestOff[i] = s.pos2off(s.bestPos[i] - 1)
				} else {
					s.BestOff[i] = 0
				}
			}
		}
	}

	s.removeNode(s.rp)
	key = head2(s.b[s.bp:])
	s.head2[key] = uint16(s.bp)
}

func (s *swd) pos2off(pos uint) uint {
	if s.bp > pos {
		return s.bp - pos
	}
	return s.bsize - (pos - s.bp)
}
