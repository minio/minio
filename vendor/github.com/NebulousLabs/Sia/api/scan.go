package api

import (
	"math/big"

	"errors"
	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/types"
)

// scanAmount scans a types.Currency from a string.
func scanAmount(amount string) (types.Currency, bool) {
	// use SetString manually to ensure that amount does not contain
	// multiple values, which would confuse fmt.Scan
	i, ok := new(big.Int).SetString(amount, 10)
	if !ok {
		return types.Currency{}, ok
	}
	return types.NewCurrency(i), true
}

// scanAddress scans a types.UnlockHash from a string.
func scanAddress(addrStr string) (addr types.UnlockHash, err error) {
	err = addr.LoadString(addrStr)
	if err != nil {
		return types.UnlockHash{}, err
	}
	return addr, nil
}

// scanHash scans a crypto.Hash from a string.
func scanHash(s string) (h crypto.Hash, err error) {
	err = h.LoadString(s)
	if err != nil {
		return crypto.Hash{}, err
	}
	return h, nil
}

// scanBool converts "true" and "false" strings to their respective
// boolean value and returns an error if conversion is not possible.
func scanBool(param string) (bool, error) {
	if param == "true" {
		return true, nil
	} else if param == "false" || len(param) == 0 {
		return false, nil
	}
	return false, errors.New("could not decode boolean: value was not true or false")
}
