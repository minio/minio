// Package mnemonics is a package that converts []byte's into human-friendly
// phrases, using common words pulled from a dictionary. The dictionary size is
// 1626, and multiple languages are supported.  Each dictionary supports
// modified phrases. Only the first few characters of each word are important.
// These characters form a unique prefix. For example, in the English
// dictionary, the unique prefix len (EnglishUniquePrefixLen) is 3, which means
// the word 'abbey' could be replaced with the word 'abbot', and the program
// would still run as expected.
//
// The primary purpose of this library is creating human-friendly
// cryptographically secure passwords. A cryptographically secure password
// needs to contain between 128 and 256 bits of entropy. Humans are typically
// incapable of generating sufficiently secure passwords without a random
// number generator, and 256-bit random numbers tend to difficult to memorize
// and even to write down (a single mistake in the writing, or even a single
// somewhat sloppy character can render the backup useless).
//
// By using a small set of common words instead of random numbers, copying
// errors are more easily spotted and memorization is also easier, without
// sacrificing password strength.
//
// The mnemonics package does not have any functions for actually generating
// entropy, it just converts existing entropy into human-friendly phrases.
package mnemonics

import (
	"errors"
	"math/big"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

const (
	// DictionarySize specifies the size of the dictionaries that are used by
	// the mnemonics package. All dictionaries are the same length so that the
	// same []byte can be encoded into multiple languages and all results will
	// resemble eachother.
	DictionarySize = 1626
)

var (
	errEmptyInput        = errors.New("input has len 0 - not valid for conversion")
	errUnknownDictionary = errors.New("language not recognized")
	errUnknownWord       = errors.New("word not found in dictionary for given language")
)

type (
	// DictionaryID is a type-safe identifier that indicates which dictionary
	// should be used.
	DictionaryID string

	// Dictionary is a DictionarySize list of words which can be used to create
	// human-friendly entropy.
	Dictionary [DictionarySize]string

	// Phrase is the human readable version of a random []byte. Most typically,
	// a phrase is displayed to the user using the String method.
	Phrase []string
)

// The conversion functions can be seen as changing the base of a number. A
// []byte can actually be viewed as a slice of base-256 numbers, and a []dict
// can be viewed as a slice of base-1626 numbers. The conversions are a little
// strange because leading 0's need to be preserved.
//
// For example, in base 256:
//
//		{0} -> 0
//		{255} -> 255
//		{0, 0} -> 256
//		{1, 0} -> 257
//		{0, 1} -> 512
//
// Every possible []byte has a unique big.Int which represents it, and every
// big.Int represents a unique []byte.

// bytesToInt converts a byte slice to a big.Int in a way that preserves
// leading 0s, and ensures there is a perfect 1:1 mapping between Int's and
// []byte's.
func bytesToInt(bs []byte) *big.Int {
	base := big.NewInt(256)
	exp := big.NewInt(1)
	result := big.NewInt(-1)
	for i := 0; i < len(bs); i++ {
		tmp := big.NewInt(int64(bs[i]))
		tmp.Add(tmp, big.NewInt(1))
		tmp.Mul(tmp, exp)
		exp.Mul(exp, base)
		result.Add(result, tmp)
	}
	return result
}

// intToBytes conversts a big.Int to a []byte, following the conventions
// documented at bytesToInt.
func intToBytes(bi *big.Int) (bs []byte) {
	base := big.NewInt(256)
	for bi.Cmp(base) >= 0 {
		i := new(big.Int).Mod(bi, base).Int64()
		bs = append(bs, byte(i))
		bi.Sub(bi, base)
		bi.Div(bi, base)
	}
	bs = append(bs, byte(bi.Int64()))
	return bs
}

// phraseToInt coverts a phrase into a big.Int, using logic similar to
// bytesToInt.
func phraseToInt(p Phrase, did DictionaryID) (*big.Int, error) {
	// Determine which dictionary to use based on the input language.
	var dict Dictionary
	var prefixLen int
	switch {
	case did == English:
		dict = englishDictionary
		prefixLen = EnglishUniquePrefixLen
	case did == German:
		dict = germanDictionary
		prefixLen = GermanUniquePrefixLen
	case did == Japanese:
		dict = japaneseDictionary
		prefixLen = JapaneseUniquePrefixLen
	default:
		return nil, errUnknownDictionary
	}

	base := big.NewInt(1626)
	exp := big.NewInt(1)
	result := big.NewInt(-1)
	for _, word := range p {
		// Normalize the input.
		word = norm.NFC.String(word)

		// Get the first prefixLen runes from the string.
		var prefix []byte
		var runeCount int
		for _, r := range word {
			encR := make([]byte, utf8.RuneLen(r))
			utf8.EncodeRune(encR, r)
			prefix = append(prefix, encR...)

			runeCount++
			if runeCount == prefixLen {
				break
			}
		}

		// Find the index associated with the phrase.
		var tmp *big.Int
		found := false
		for j, word := range dict {
			if strings.HasPrefix(word, string(prefix)) {
				tmp = big.NewInt(int64(j))
				found = true
				break
			}
		}
		if !found {
			return nil, errUnknownWord
		}

		// Add the index to the int.
		tmp.Add(tmp, big.NewInt(1))
		tmp.Mul(tmp, exp)
		exp.Mul(exp, base)
		result.Add(result, tmp)
	}
	return result, nil
}

// intToPhrase converts a phrase into a big.Int, working in a fashion similar
// to bytesToInt.
func intToPhrase(bi *big.Int, did DictionaryID) (p Phrase, err error) {
	// Determine which dictionary to use based on the input language.
	var dict Dictionary
	switch {
	case did == English:
		dict = englishDictionary
	case did == German:
		dict = germanDictionary
	case did == Japanese:
		dict = japaneseDictionary
	default:
		return nil, errUnknownDictionary
	}

	base := big.NewInt(DictionarySize)
	for bi.Cmp(base) >= 0 {
		i := new(big.Int).Mod(bi, base).Int64()
		p = append(p, dict[i])
		bi.Sub(bi, base)
		bi.Div(bi, base)
	}
	p = append(p, dict[bi.Int64()])
	return p, nil
}

// ToPhrase converts an input []byte to a human-friendly phrase. The conversion
// is reversible.
func ToPhrase(entropy []byte, did DictionaryID) (Phrase, error) {
	if len(entropy) == 0 {
		return nil, errEmptyInput
	}
	intEntropy := bytesToInt(entropy)
	return intToPhrase(intEntropy, did)
}

// FromPhrase converts an input phrase back to the original []byte.
func FromPhrase(p Phrase, did DictionaryID) ([]byte, error) {
	if len(p) == 0 {
		return nil, errEmptyInput
	}

	intEntropy, err := phraseToInt(p, did)
	if err != nil {
		return nil, err
	}
	return intToBytes(intEntropy), nil
}

// FromString converts an input string into a phrase, and then calls
// 'FromPhrase'.
func FromString(str string, did DictionaryID) ([]byte, error) {
	phrase := Phrase(strings.Split(str, " "))
	return FromPhrase(phrase, did)
}

// String combines a phrase into a single string by concatenating the
// individual words with space separation.
func (p Phrase) String() string {
	return strings.Join(p, " ")
}
