package pubsub

import (
	"math"
	"math/bits"
)

// Mask allows filtering by a bitset mask.
type Mask uint64

const (
	// MaskAll is the mask for all entries.
	MaskAll Mask = math.MaxUint64
)

// MaskFromMaskable extracts mask from an interface.
func MaskFromMaskable(m Maskable) Mask {
	return Mask(m.Mask())
}

// Contains returns whether *all* flags in other is present in t.
func (t Mask) Contains(other Mask) bool {
	return t&other == other
}

// Overlaps returns whether *any* flags in t overlaps with other.
func (t Mask) Overlaps(other Mask) bool {
	return t&other != 0
}

// SingleType returns whether t has a single type set.
func (t Mask) SingleType() bool {
	return bits.OnesCount64(uint64(t)) == 1
}

// FromUint64 will set a mask to the uint64 value.
func (t *Mask) FromUint64(m uint64) {
	*t = Mask(m)
}

// Merge will merge other into t.
func (t *Mask) Merge(other Mask) {
	*t |= other
}

// MergeMaskable will merge other into t.
func (t *Mask) MergeMaskable(other Maskable) {
	*t |= Mask(other.Mask())
}

// SetIf will add other if b is true.
func (t *Mask) SetIf(b bool, other Mask) {
	if b {
		*t |= other
	}
}

// Mask returns the mask as a uint64.
func (t Mask) Mask() uint64 {
	return uint64(t)
}

// Maskable implementations must return their mask as a 64 bit uint.
type Maskable interface {
	Mask() uint64
}
