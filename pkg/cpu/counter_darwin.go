package cpu

import (
	"errors"
)

func newCounter() (counter, error) {
	return counter{}, errors.New("cpu metrics not implemented for darwin platform")
}

func (c counter) now() time.Time {
	return time.Time{}
}
