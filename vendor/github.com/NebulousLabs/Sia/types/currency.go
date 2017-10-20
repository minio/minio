package types

// currency.go defines the internal currency object. One design goal of the
// currency type is immutability: the currency type should be safe to pass
// directly to other objects and packages. The currency object should never
// have a negative value. The currency should never overflow. There is a
// maximum size value that can be encoded (around 10^10^20), however exceeding
// this value will not result in overflow.

import (
	"errors"
	"math"
	"math/big"

	"github.com/NebulousLabs/Sia/build"
)

type (
	// A Currency represents a number of siacoins or siafunds. Internally, a
	// Currency value is unbounded; however, Currency values sent over the wire
	// protocol are subject to a maximum size of 255 bytes (approximately 10^614).
	// Unlike the math/big library, whose methods modify their receiver, all
	// arithmetic Currency methods return a new value. Currency cannot be negative.
	Currency struct {
		i big.Int
	}
)

var (
	// ZeroCurrency defines a currency of value zero.
	ZeroCurrency = NewCurrency64(0)

	// ErrNegativeCurrency is the error that is returned if performing an
	// operation results in a negative currency.
	ErrNegativeCurrency = errors.New("negative currency not allowed")

	// ErrUint64Overflow is the error that is returned if converting to a
	// unit64 would cause an overflow.
	ErrUint64Overflow = errors.New("cannot return the uint64 of this currency - result is an overflow")
)

// NewCurrency creates a Currency value from a big.Int. Undefined behavior
// occurs if a negative input is used.
func NewCurrency(b *big.Int) (c Currency) {
	if b.Sign() < 0 {
		build.Critical(ErrNegativeCurrency)
	} else {
		c.i = *b
	}
	return
}

// NewCurrency64 creates a Currency value from a uint64.
func NewCurrency64(x uint64) (c Currency) {
	c.i.SetUint64(x)
	return
}

// Add returns a new Currency value c = x + y
func (x Currency) Add(y Currency) (c Currency) {
	c.i.Add(&x.i, &y.i)
	return
}

// Big returns the value of c as a *big.Int. Importantly, it does not provide
// access to the c's internal big.Int object, only a copy.
func (c Currency) Big() *big.Int {
	return new(big.Int).Set(&c.i)
}

// Cmp compares two Currency values. The return value follows the convention
// of math/big.
func (x Currency) Cmp(y Currency) int {
	return x.i.Cmp(&y.i)
}

// Cmp64 compares x to a uint64. The return value follows the convention of
// math/big.
func (x Currency) Cmp64(y uint64) int {
	return x.Cmp(NewCurrency64(y))
}

// Div returns a new Currency value c = x / y.
func (x Currency) Div(y Currency) (c Currency) {
	c.i.Div(&x.i, &y.i)
	return
}

// Div64 returns a new Currency value c = x / y.
func (x Currency) Div64(y uint64) (c Currency) {
	c.i.Div(&x.i, new(big.Int).SetUint64(y))
	return
}

// Equals returns true if x and y have the same value.
func (x Currency) Equals(y Currency) bool {
	return x.Cmp(y) == 0
}

// Equals64 returns true if x and y have the same value.
func (x Currency) Equals64(y uint64) bool {
	return x.Cmp64(y) == 0
}

// Mul returns a new Currency value c = x * y.
func (x Currency) Mul(y Currency) (c Currency) {
	c.i.Mul(&x.i, &y.i)
	return
}

// Mul64 returns a new Currency value c = x * y.
func (x Currency) Mul64(y uint64) (c Currency) {
	c.i.Mul(&x.i, new(big.Int).SetUint64(y))
	return
}

// COMPATv0.4.0 - until the first 10e3 blocks have been archived, MulFloat is
// needed while verifying the first set of blocks.
//
// MulFloat returns a new Currency value y = c * x, where x is a float64.
// Behavior is undefined when x is negative.
func (x Currency) MulFloat(y float64) (c Currency) {
	if y < 0 {
		build.Critical(ErrNegativeCurrency)
	} else {
		cRat := new(big.Rat).Mul(
			new(big.Rat).SetInt(&x.i),
			new(big.Rat).SetFloat64(y),
		)
		c.i.Div(cRat.Num(), cRat.Denom())
	}
	return
}

// MulRat returns a new Currency value c = x * y, where y is a big.Rat.
func (x Currency) MulRat(y *big.Rat) (c Currency) {
	if y.Sign() < 0 {
		build.Critical(ErrNegativeCurrency)
	} else {
		c.i.Mul(&x.i, y.Num())
		c.i.Div(&c.i, y.Denom())
	}
	return
}

// MulTax returns a new Currency value c = x * 0.039, where 0.039 is a big.Rat.
func (x Currency) MulTax() (c Currency) {
	c.i.Mul(&x.i, big.NewInt(39))
	c.i.Div(&c.i, big.NewInt(1000))
	return c
}

// RoundDown returns the largest multiple of y <= x.
func (x Currency) RoundDown(y Currency) (c Currency) {
	diff := new(big.Int).Mod(&x.i, &y.i)
	c.i.Sub(&x.i, diff)
	return
}

// IsZero returns true if the value is 0, false otherwise.
func (c Currency) IsZero() bool {
	return c.i.Sign() <= 0
}

// Sqrt returns a new Currency value y = sqrt(c). Result is rounded down to the
// nearest integer.
func (x Currency) Sqrt() (c Currency) {
	f, _ := new(big.Rat).SetInt(&x.i).Float64()
	sqrt := new(big.Rat).SetFloat64(math.Sqrt(f))
	c.i.Div(sqrt.Num(), sqrt.Denom())
	return
}

// Sub returns a new Currency value c = x - y. Behavior is undefined when
// x < y.
func (x Currency) Sub(y Currency) (c Currency) {
	if x.Cmp(y) < 0 {
		c = x
		build.Critical(ErrNegativeCurrency)
	} else {
		c.i.Sub(&x.i, &y.i)
	}
	return
}

// Uint64 converts a Currency to a uint64. An error is returned because this
// function is sometimes called on values that can be determined by users -
// rather than have all user-facing points do input checking, the input
// checking should happen at the base type. This minimizes the chances of a
// rogue user causing a build.Critical to be triggered.
func (c Currency) Uint64() (u uint64, err error) {
	if c.Cmp(NewCurrency64(math.MaxUint64)) > 0 {
		return 0, ErrUint64Overflow
	}
	return c.Big().Uint64(), nil
}
