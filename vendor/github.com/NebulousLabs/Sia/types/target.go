package types

// target.go defines the target type and implements a few helper functions for
// manipulating the target type.

import (
	"errors"
	"math/big"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/crypto"
)

type (
	// A Target is a hash that a block's ID must be "less than" in order for
	// the block to be considered valid. Miners vary the block's 'Nonce' field
	// in order to brute-force such an ID. The inverse of a Target is called
	// the "difficulty," because it is proportional to the amount of time
	// required to brute-force the Target.
	Target crypto.Hash
)

var (
	ErrNegativeTarget = errors.New("negative value used when converting to target")
)

// AddDifficulties returns the resulting target with the difficulty of 'x' and
// 'y' are added together. Note that the difficulty is the inverse of the
// target. The sum is defined by:
//		sum(x, y) = 1/(1/x + 1/y)
func (x Target) AddDifficulties(y Target) (t Target) {
	sumDifficulty := new(big.Rat).Add(x.Inverse(), y.Inverse())
	return RatToTarget(new(big.Rat).Inv(sumDifficulty))
}

// Cmp compares the difficulties of two targets. Note that the difficulty is
// the inverse of the target. The results are as follows:
//		-1 if x <  y
//		 0 if x == y
//		+1 if x >  y
func (x Target) Cmp(y Target) int {
	return x.Int().Cmp(y.Int())
}

// Difficulty returns the difficulty associated with a given target.
func (t Target) Difficulty() Currency {
	if t == (Target{}) {
		return NewCurrency(RootDepth.Int())
	}
	return NewCurrency(new(big.Int).Div(RootDepth.Int(), t.Int()))
}

// Int converts a Target to a big.Int.
func (t Target) Int() *big.Int {
	return new(big.Int).SetBytes(t[:])
}

// IntToTarget converts a big.Int to a Target. Negative inputs trigger a panic.
func IntToTarget(i *big.Int) (t Target) {
	// Check for negatives.
	if i.Sign() < 0 {
		if build.DEBUG {
			panic(ErrNegativeTarget)
		}
	} else {
		// In the event of overflow, return the maximum.
		if i.BitLen() > 256 {
			return RootDepth
		}
		b := i.Bytes()
		offset := len(t[:]) - len(b)
		copy(t[offset:], b)
	}
	return
}

// Inverse returns the inverse of a Target as a big.Rat
func (t Target) Inverse() *big.Rat {
	return new(big.Rat).Inv(t.Rat())
}

// Mul multiplies the difficulty of a target by y. The product is defined by:
//		y / x
func (x Target) MulDifficulty(y *big.Rat) (t Target) {
	product := new(big.Rat).Mul(y, x.Inverse())
	product = product.Inv(product)
	return RatToTarget(product)
}

// Rat converts a Target to a big.Rat.
func (t Target) Rat() *big.Rat {
	return new(big.Rat).SetInt(t.Int())
}

// RatToTarget converts a big.Rat to a Target.
func RatToTarget(r *big.Rat) (t Target) {
	if r.Num().Sign() < 0 {
		if build.DEBUG {
			panic(ErrNegativeTarget)
		}
	} else {
		i := new(big.Int).Div(r.Num(), r.Denom())
		t = IntToTarget(i)
	}
	return
}

// SubtractDifficulties returns the resulting target with the difficulty of 'x'
// is subtracted from the target with difficulty 'y'. Note that the difficulty
// is the inverse of the target. The difference is defined by:
//		sum(x, y) = 1/(1/x - 1/y)
func (x Target) SubtractDifficulties(y Target) (t Target) {
	sumDifficulty := new(big.Rat).Sub(x.Inverse(), y.Inverse())
	return RatToTarget(new(big.Rat).Inv(sumDifficulty))
}
