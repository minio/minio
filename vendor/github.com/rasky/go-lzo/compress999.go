package lzo

type compressor struct {
	in []byte
	ip int
	bp int

	// stats
	matchBytes int
	litBytes   int
	lazy       int

	r1lit int
	r2lit int
	m1am  uint
	m2m   uint
	m1bm  uint
	m3m   uint
	m4m   uint
	lit1r uint
	lit2r uint
	lit3r uint

	r1mlen int

	lastmlen int
	lastmoff int
	textsize uint
	mlen     int
	moff     int
	look     uint
}

func (ctx *compressor) codeMatch(out []byte, mlen int, moff int) []byte {
	xlen := mlen
	xoff := moff
	ctx.matchBytes += mlen

	switch {
	case mlen == 2:
		if moff > m1_MAX_OFFSET {
			panic("codeMatch: mlen 2: moff error")
		}
		if ctx.r1lit < 1 || ctx.r1lit >= 4 {
			panic("codeMatch: mlen 2: r1lit error")
		}
		moff -= 1
		out = append(out,
			m1_MARKER|byte((moff&3)<<2),
			byte(moff>>2))
		ctx.m1am++
	case mlen <= m2_MAX_LEN && moff <= m2_MAX_OFFSET:
		if mlen < 3 {
			panic("codeMatch: m2: mlen error")
		}
		moff -= 1
		out = append(out,
			byte((mlen-1)<<5|(moff&7)<<2),
			byte(moff>>3))
		if out[len(out)-2] < m2_MARKER {
			panic("codeMatch: m2: invalid marker")
		}
		ctx.m2m++
	case mlen == m2_MIN_LEN && moff <= mX_MAX_OFFSET && ctx.r1lit >= 4:
		if mlen != 3 {
			panic("codeMatch: m2min: invalid mlen")
		}
		if moff <= m2_MAX_OFFSET {
			panic("codeMatch: m2min: invalid moff")
		}
		moff -= 1 + m2_MAX_OFFSET
		out = append(out,
			byte(m1_MARKER|((moff&3)<<2)),
			byte(moff>>2))
		ctx.m1bm++
	case moff <= m3_MAX_OFFSET:
		if mlen < 3 {
			panic("codeMatch: m3max: invalid mlen")
		}
		moff -= 1
		if mlen <= m3_MAX_LEN {
			out = append(out, byte(m3_MARKER|(mlen-2)))
		} else {
			mlen -= m3_MAX_LEN
			out = append(out, byte(m3_MARKER|0))
			out = appendMulti(out, mlen)
		}
		out = append(out, byte(moff<<2), byte(moff>>6))
		ctx.m3m++
	default:
		if mlen < 3 {
			panic("codeMatch: default: invalid mlen")
		}
		if moff <= 0x4000 || moff >= 0xc000 {
			panic("codeMatch: default: invalid moff")
		}
		moff -= 0x4000
		k := (moff & 0x4000) >> 11
		if mlen <= m4_MAX_LEN {
			out = append(out, byte(m4_MARKER|k|(mlen-2)))
		} else {
			mlen -= m4_MAX_LEN
			out = append(out, byte(m4_MARKER|k|0))
			out = appendMulti(out, mlen)
		}
		out = append(out, byte(moff<<2), byte(moff>>6))
		ctx.m4m++
	}

	ctx.lastmlen = xlen
	ctx.lastmoff = xoff
	return out
}

func (ctx *compressor) storeRun(out []byte, ii int, t int) []byte {
	ctx.litBytes += t

	if len(out) == 0 && t <= 238 {
		out = append(out, byte(17+t))
	} else if t <= 3 {
		out[len(out)-2] |= byte(t)
		ctx.lit1r++
	} else if t <= 18 {
		out = append(out, byte(t-3))
		ctx.lit2r++
	} else {
		out = append(out, 0)
		out = appendMulti(out, t-18)
		ctx.lit3r++
	}

	out = append(out, ctx.in[ii:ii+t]...)
	return out
}

func (ctx *compressor) codeRun(out []byte, ii int, lit int, mlen int) []byte {
	if lit > 0 {
		if mlen < 2 {
			panic("codeRun: invalid mlen")
		}
		out = ctx.storeRun(out, ii, lit)
		ctx.r1mlen = mlen
		ctx.r1lit = lit
	} else {
		if mlen < 3 {
			panic("codeRun: invalid mlen")
		}
		ctx.r1mlen = 0
		ctx.r1lit = 0
	}
	return out
}

func (ctx *compressor) lenOfCodedMatch(mlen int, moff int, lit int) int {
	switch {
	case mlen < 2:
		return 0
	case mlen == 2:
		if moff <= m1_MAX_OFFSET && lit > 0 && lit < 4 {
			return 2
		}
		return 0
	case mlen <= m2_MAX_LEN && moff <= m2_MAX_OFFSET:
		return 2
	case mlen == m2_MIN_LEN && moff <= mX_MAX_OFFSET && lit >= 4:
		return 2
	case moff <= m3_MAX_OFFSET:
		if mlen <= m3_MAX_LEN {
			return 3
		}
		n := 4
		mlen -= m3_MAX_LEN
		for mlen > 255 {
			mlen -= 255
			n++
		}
		return n
	case moff <= m4_MAX_OFFSET:
		if mlen <= m4_MAX_LEN {
			return 3
		}
		n := 4
		mlen -= m4_MAX_LEN
		for mlen > 255 {
			mlen -= 255
			n++
		}
		return n
	default:
		return 0
	}
}

func (ctx *compressor) minGain(ahead int,
	lit1, lit2 int, l1, l2, l3 int) int {

	if ahead <= 0 {
		panic("minGain: invalid ahead")
	}
	mingain := int(ahead)
	if lit1 <= 3 {
		if lit2 > 3 {
			mingain += 2
		}
	} else if lit1 <= 18 {
		if lit2 > 18 {
			mingain += 1
		}
	}

	mingain += int((l2 - l1) * 2)
	if l3 != 0 {
		mingain -= int((ahead - l3) * 2)
	}
	if mingain < 0 {
		mingain = 0
	}
	return mingain
}

type parms struct {
	TryLazy  int
	GoodLen  uint
	MaxLazy  uint
	NiceLen  uint
	MaxChain uint
	Flags    uint32
}

func compress999(in []byte, p parms) []byte {
	ctx := compressor{}
	swd := swd{}

	if p.TryLazy < 0 {
		p.TryLazy = 1
	}
	if p.GoodLen == 0 {
		p.GoodLen = 32
	}
	if p.MaxLazy == 0 {
		p.MaxLazy = 32
	}
	if p.MaxChain == 0 {
		p.MaxChain = cSWD_MAX_CHAIN
	}

	ctx.in = in

	out := make([]byte, 0, len(in)/2)
	ii := 0
	lit := 0

	ctx.initMatch(&swd, p.Flags)
	if p.MaxChain > 0 {
		swd.MaxChain = p.MaxChain
	}
	if p.NiceLen > 0 {
		swd.NiceLength = p.NiceLen
	}

	ctx.findMatch(&swd, 0, 0)
	for ctx.look > 0 {
		mlen := ctx.mlen
		moff := ctx.moff
		if ctx.bp != ctx.ip-int(ctx.look) {
			panic("assert: compress: invalid bp")
		}
		if ctx.bp < 0 {
			panic("assert: compress: negative bp")
		}
		if lit == 0 {
			ii = ctx.bp
		}
		if ii+lit != ctx.bp {
			panic("assert: compress: invalid ii")
		}
		if swd.BChar != int(ctx.in[ctx.bp]) {
			panic("assert: compress: invalid bchar")
		}

		if mlen < 2 ||
			(mlen == 2 && (moff > m1_MAX_OFFSET || lit == 0 || lit >= 4)) ||
			(mlen == 2 && len(out) == 0) ||
			(len(out) == 0 && lit == 0) {
			// literal
			mlen = 0
		} else if mlen == m2_MIN_LEN {
			if moff > mX_MAX_OFFSET && lit >= 4 {
				mlen = 0
			}
		}

		if mlen == 0 {
			// literal
			lit++
			swd.MaxChain = p.MaxChain
			ctx.findMatch(&swd, 1, 0)
			continue
		}

		// a match
		if swd.UseBestOff {
			mlen, moff = ctx.betterMatch(&swd, mlen, moff)
		}

		ctx.assertMatch(&swd, mlen, moff)

		// check if we want to try a lazy match
		ahead := 0
		l1 := 0
		maxahead := 0
		if p.TryLazy != 0 && mlen < int(p.MaxLazy) {
			l1 = ctx.lenOfCodedMatch(mlen, moff, lit)
			if l1 == 0 {
				panic("assert: compress: invalid len of coded match")
			}
			maxahead = p.TryLazy
			if maxahead > l1-1 {
				maxahead = l1 - 1
			}
		}

		matchdone := false
		for ahead < maxahead && int(ctx.look) > mlen {
			if mlen >= int(p.GoodLen) {
				swd.MaxChain = p.MaxChain >> 2
			} else {
				swd.MaxChain = p.MaxChain
			}
			ctx.findMatch(&swd, 1, 0)
			ahead++
			if ctx.look <= 0 {
				panic("assert: compress: invalid look")
			}
			if ii+lit+ahead != ctx.bp {
				panic("assert: compress: invalid bp")
			}
			if ctx.mlen < mlen {
				continue
			}
			if ctx.mlen == mlen && ctx.moff >= moff {
				continue
			}
			if swd.UseBestOff {
				ctx.mlen, ctx.moff = ctx.betterMatch(&swd, ctx.mlen, ctx.moff)
			}
			l2 := ctx.lenOfCodedMatch(ctx.mlen, ctx.moff, lit+ahead)
			if l2 == 0 {
				continue
			}
			l3 := 0
			if len(out) > 0 {
				l3 = ctx.lenOfCodedMatch(ahead, moff, lit)
			}
			mingain := ctx.minGain(ahead, lit, lit+ahead, l1, l2, l3)
			if ctx.mlen >= mlen+mingain {
				ctx.lazy++
				ctx.assertMatch(&swd, ctx.mlen, ctx.moff)

				if l3 > 0 {
					out = ctx.codeRun(out, ii, lit, ahead)
					lit = 0
					out = ctx.codeMatch(out, ahead, moff)
				} else {
					lit += ahead
					if ii+lit != ctx.bp {
						panic("assert: compress: invalid bp after l3")
					}
				}
				matchdone = true
				break
			}
		}

		if !matchdone {
			if ii+lit+ahead != ctx.bp {
				panic("assert: compress: invalid bp out of for loop")
			}

			out = ctx.codeRun(out, ii, lit, mlen)
			lit = 0
			out = ctx.codeMatch(out, mlen, moff)
			swd.MaxChain = p.MaxChain
			ctx.findMatch(&swd, uint(mlen), uint(1+ahead))
		}
	}

	if lit > 0 {
		out = ctx.storeRun(out, ii, lit)
	}
	out = append(out, m4_MARKER|1, 0, 0)
	if ctx.litBytes+ctx.matchBytes != len(ctx.in) {
		panic("assert: compress999: not processed full input")
	}
	return out
}

var fixedLevels = [...]parms{
	{0, 0, 0, 8, 4, 0},
	{0, 0, 0, 16, 8, 0},
	{0, 0, 0, 32, 16, 0},
	{1, 4, 4, 16, 16, 0},
	{1, 8, 16, 32, 32, 0},
	{1, 8, 16, 128, 128, 0},
	{2, 8, 32, 128, 256, 0},
	{2, 32, 128, cSWD_F, 2048, 1},
	{2, cSWD_F, cSWD_F, cSWD_F, 4096, 1},
}

func Compress1X999Level(in []byte, level int) []byte {
	return compress999(in, fixedLevels[level-1])
}

func Compress1X999(in []byte) []byte {
	return Compress1X999Level(in, 9)
}
