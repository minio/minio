package lzo

func (ctx *compressor) initMatch(s *swd, flags uint32) {
	s.ctx = ctx
	s.init()
	if flags&1 != 0 {
		s.UseBestOff = true
	}
}

func (ctx *compressor) findMatch(s *swd, thislen uint, skip uint) {
	if skip > 0 {
		if thislen < skip {
			panic("assert: findMatch: invalid thislen")
		}
		s.accept(thislen - skip)
		ctx.textsize += thislen - skip + 1
	} else {
		if thislen > 1 {
			panic("assert: findMatch: invalid thislen")
		}
		ctx.textsize += thislen - skip
	}

	s.MLen = cSWD_THRESHOLD
	s.MOff = 0
	for i := 0; i < len(s.bestPos); i++ {
		s.bestPos[i] = 0
	}

	s.findbest()
	ctx.mlen = int(s.MLen)
	ctx.moff = int(s.MOff)

	s.getbyte()
	if s.BChar < 0 {
		ctx.look = 0
		ctx.mlen = 0
	} else {
		ctx.look = s.Look + 1
	}

	ctx.bp = ctx.ip - int(ctx.look)
}

func (ctx *compressor) betterMatch(s *swd, imlen, imoff int) (mlen int, moff int) {
	mlen, moff = imlen, imoff
	if mlen <= m2_MIN_LEN {
		return
	}
	if moff <= m2_MAX_OFFSET {
		return
	}

	if moff > m2_MAX_OFFSET && mlen >= m2_MIN_LEN+1 && mlen <= m2_MAX_LEN+1 &&
		s.BestOff[mlen-1] > 0 && s.BestOff[mlen-1] <= m2_MAX_OFFSET {
		mlen -= 1
		moff = int(s.BestOff[mlen])
		return
	}

	if moff > m3_MAX_OFFSET && mlen >= m4_MAX_LEN+1 && mlen <= m2_MAX_LEN+2 &&
		s.BestOff[mlen-2] > 0 && s.BestOff[mlen-2] <= m2_MAX_OFFSET {
		mlen -= 2
		moff = int(s.BestOff[mlen])
		return
	}

	if moff > m3_MAX_OFFSET && mlen >= m4_MAX_LEN+1 && mlen <= m3_MAX_LEN+1 &&
		s.BestOff[mlen-1] > 0 && s.BestOff[mlen-1] <= m3_MAX_OFFSET {
		mlen -= 1
		moff = int(s.BestOff[mlen])
		return
	}

	return
}

func assertMemcmp(b1, b2 []byte, l int) {
	b1 = b1[:l]
	b2 = b2[:l]
	for i := 0; i < len(b1); i++ {
		if b1[i] != b2[i] {
			panic("assertMemcmp: dosn't match")
		}
	}
}

func (ctx *compressor) assertMatch(s *swd, mlen, moff int) {
	if mlen < 2 {
		panic("assertMatch: invalid mlen")
	}
	if moff <= ctx.bp {
		if ctx.bp-moff+mlen >= ctx.ip {
			panic("assertMatch: invalid bp")
		}
		assertMemcmp(ctx.in[ctx.bp:], ctx.in[ctx.bp-moff:], mlen)
	} else {
		panic("dict should not exit")
	}
}
