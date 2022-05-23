package pubsub

import "math/bits"

// Mask allows filtering by a bitset mask.
type Mask uint32

// Contains returns whether all flags in other is present in t.
func (t Mask) Contains(other Mask) bool {
	return t&other == other
}

// Overlaps returns whether any flags in t overlaps with other.
func (t Mask) Overlaps(other Mask) bool {
	return t&other != 0
}

// SingleType returns whether t has a single type set.
func (t Mask) SingleType() bool {
	// Include
	return bits.OnesCount32(uint32(t)) == 1
}

// Merge will merge other into t.
func (t *Mask) Merge(other Mask) {
	*t = *t | other
}

// SetIf will add other if b is true.
func (t *Mask) SetIf(b bool, other Mask) {
	if b {
		*t = *t | other
	}
}

// Maskable implementations must return their mask as a 32 bit uint.
type Maskable interface {
	Mask() uint32
}
