package types

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/encoding"
)

// sanityCheckWriter checks that the bytes written to w exactly match the
// bytes in buf.
type sanityCheckWriter struct {
	w   io.Writer
	buf *bytes.Buffer
}

func (s sanityCheckWriter) Write(p []byte) (int, error) {
	if !bytes.Equal(p, s.buf.Next(len(p))) {
		panic("encoding mismatch")
	}
	return s.w.Write(p)
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (b Block) MarshalSia(w io.Writer) error {
	if build.DEBUG {
		// Sanity check: compare against the old encoding
		buf := new(bytes.Buffer)
		encoding.NewEncoder(buf).EncodeAll(
			b.ParentID,
			b.Nonce,
			b.Timestamp,
			b.MinerPayouts,
			b.Transactions,
		)
		w = sanityCheckWriter{w, buf}
	}

	w.Write(b.ParentID[:])
	w.Write(b.Nonce[:])
	encoding.WriteUint64(w, uint64(b.Timestamp))
	encoding.WriteInt(w, len(b.MinerPayouts))
	for i := range b.MinerPayouts {
		b.MinerPayouts[i].MarshalSia(w)
	}
	encoding.WriteInt(w, len(b.Transactions))
	for i := range b.Transactions {
		if err := b.Transactions[i].MarshalSia(w); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalSia implements the encoding.SiaUnmarshaler interface.
func (b *Block) UnmarshalSia(r io.Reader) error {
	io.ReadFull(r, b.ParentID[:])
	io.ReadFull(r, b.Nonce[:])
	tsBytes := make([]byte, 8)
	io.ReadFull(r, tsBytes)
	b.Timestamp = Timestamp(encoding.DecUint64(tsBytes))
	return encoding.NewDecoder(r).DecodeAll(&b.MinerPayouts, &b.Transactions)
}

// MarshalJSON marshales a block id as a hex string.
func (bid BlockID) MarshalJSON() ([]byte, error) {
	return json.Marshal(bid.String())
}

// String prints the block id in hex.
func (bid BlockID) String() string {
	return fmt.Sprintf("%x", bid[:])
}

// UnmarshalJSON decodes the json hex string of the block id.
func (bid *BlockID) UnmarshalJSON(b []byte) error {
	return (*crypto.Hash)(bid).UnmarshalJSON(b)
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (cf CoveredFields) MarshalSia(w io.Writer) error {
	if cf.WholeTransaction {
		w.Write([]byte{1})
	} else {
		w.Write([]byte{0})
	}
	fields := [][]uint64{
		cf.SiacoinInputs,
		cf.SiacoinOutputs,
		cf.FileContracts,
		cf.FileContractRevisions,
		cf.StorageProofs,
		cf.SiafundInputs,
		cf.SiafundOutputs,
		cf.MinerFees,
		cf.ArbitraryData,
		cf.TransactionSignatures,
	}
	for _, f := range fields {
		encoding.WriteInt(w, len(f))
		for _, u := range f {
			if err := encoding.WriteUint64(w, u); err != nil {
				return err
			}
		}
	}
	return nil
}

// MarshalSiaSize returns the encoded size of cf.
func (cf CoveredFields) MarshalSiaSize() (size int) {
	size += 1 // WholeTransaction
	size += 8 + len(cf.SiacoinInputs)*8
	size += 8 + len(cf.SiacoinOutputs)*8
	size += 8 + len(cf.FileContracts)*8
	size += 8 + len(cf.FileContractRevisions)*8
	size += 8 + len(cf.StorageProofs)*8
	size += 8 + len(cf.SiafundInputs)*8
	size += 8 + len(cf.SiafundOutputs)*8
	size += 8 + len(cf.MinerFees)*8
	size += 8 + len(cf.ArbitraryData)*8
	size += 8 + len(cf.TransactionSignatures)*8
	return
}

// MarshalJSON implements the json.Marshaler interface.
func (c Currency) MarshalJSON() ([]byte, error) {
	// Must enclosed the value in quotes; otherwise JS will convert it to a
	// double and lose precision.
	return []byte(`"` + c.String() + `"`), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface. An error is
// returned if a negative number is provided.
func (c *Currency) UnmarshalJSON(b []byte) error {
	// UnmarshalJSON does not expect quotes
	b = bytes.Trim(b, `"`)
	err := c.i.UnmarshalJSON(b)
	if err != nil {
		return err
	}
	if c.i.Sign() < 0 {
		c.i = *big.NewInt(0)
		return ErrNegativeCurrency
	}
	return nil
}

// MarshalSia implements the encoding.SiaMarshaler interface. It writes the
// byte-slice representation of the Currency's internal big.Int to w. Note
// that as the bytes of the big.Int correspond to the absolute value of the
// integer, there is no way to marshal a negative Currency.
func (c Currency) MarshalSia(w io.Writer) error {
	return encoding.WritePrefix(w, c.i.Bytes())
}

// MarshalSiaSize returns the encoded size of c.
func (c Currency) MarshalSiaSize() int {
	// from math/big/arith.go
	const (
		_m    = ^big.Word(0)
		_logS = _m>>8&1 + _m>>16&1 + _m>>32&1
		_S    = 1 << _logS // number of bytes per big.Word
	)

	// start with the number of Words * number of bytes per Word, then
	// subtract trailing bytes that are 0
	bits := c.i.Bits()
	size := len(bits) * _S
zeros:
	for i := len(bits) - 1; i >= 0; i-- {
		for j := _S - 1; j >= 0; j-- {
			if (bits[i] >> uintptr(j*8)) != 0 {
				break zeros
			}
			size--
		}
	}
	return 8 + size // account for length prefix
}

// UnmarshalSia implements the encoding.SiaUnmarshaler interface.
func (c *Currency) UnmarshalSia(r io.Reader) error {
	b, err := encoding.ReadPrefix(r, 256)
	if err != nil {
		return err
	}
	var dec Currency
	dec.i.SetBytes(b)
	*c = dec
	return nil
}

// HumanString prints the Currency using human readable units. The unit used
// will be the largest unit that results in a value greater than 1. The value is
// rounded to 4 significant digits.
func (c Currency) HumanString() string {
	pico := SiacoinPrecision.Div64(1e12)
	if c.Cmp(pico) < 0 {
		return c.String() + " H"
	}

	// iterate until we find a unit greater than c
	mag := pico
	unit := ""
	for _, unit = range []string{"pS", "nS", "uS", "mS", "SC", "KS", "MS", "GS", "TS"} {
		if c.Cmp(mag.Mul64(1e3)) < 0 {
			break
		} else if unit != "TS" {
			// don't want to perform this multiply on the last iter; that
			// would give us 1.235 TS instead of 1235 TS
			mag = mag.Mul64(1e3)
		}
	}

	num := new(big.Rat).SetInt(c.Big())
	denom := new(big.Rat).SetInt(mag.Big())
	res, _ := new(big.Rat).Mul(num, denom.Inv(denom)).Float64()

	return fmt.Sprintf("%.4g %s", res, unit)
}

// String implements the fmt.Stringer interface.
func (c Currency) String() string {
	return c.i.String()
}

// Scan implements the fmt.Scanner interface, allowing Currency values to be
// scanned from text.
func (c *Currency) Scan(s fmt.ScanState, ch rune) error {
	var dec Currency
	err := dec.i.Scan(s, ch)
	if err != nil {
		return err
	}
	if dec.i.Sign() < 0 {
		return ErrNegativeCurrency
	}
	*c = dec
	return nil
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (fc FileContract) MarshalSia(w io.Writer) error {
	encoding.WriteUint64(w, fc.FileSize)
	w.Write(fc.FileMerkleRoot[:])
	encoding.WriteUint64(w, uint64(fc.WindowStart))
	encoding.WriteUint64(w, uint64(fc.WindowEnd))
	fc.Payout.MarshalSia(w)
	encoding.WriteInt(w, len(fc.ValidProofOutputs))
	for _, sco := range fc.ValidProofOutputs {
		sco.MarshalSia(w)
	}
	encoding.WriteInt(w, len(fc.MissedProofOutputs))
	for _, sco := range fc.MissedProofOutputs {
		sco.MarshalSia(w)
	}
	w.Write(fc.UnlockHash[:])
	return encoding.WriteUint64(w, fc.RevisionNumber)
}

// MarshalSiaSize returns the encoded size of fc.
func (fc FileContract) MarshalSiaSize() (size int) {
	size += 8 // FileSize
	size += len(fc.FileMerkleRoot)
	size += 8 + 8 // WindowStart + WindowEnd
	size += fc.Payout.MarshalSiaSize()
	size += 8
	for _, sco := range fc.ValidProofOutputs {
		size += sco.Value.MarshalSiaSize()
		size += len(sco.UnlockHash)
	}
	size += 8
	for _, sco := range fc.MissedProofOutputs {
		size += sco.Value.MarshalSiaSize()
		size += len(sco.UnlockHash)
	}
	size += len(fc.UnlockHash)
	size += 8 // RevisionNumber
	return
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (fcr FileContractRevision) MarshalSia(w io.Writer) error {
	w.Write(fcr.ParentID[:])
	fcr.UnlockConditions.MarshalSia(w)
	encoding.WriteUint64(w, fcr.NewRevisionNumber)
	encoding.WriteUint64(w, fcr.NewFileSize)
	w.Write(fcr.NewFileMerkleRoot[:])
	encoding.WriteUint64(w, uint64(fcr.NewWindowStart))
	encoding.WriteUint64(w, uint64(fcr.NewWindowEnd))
	encoding.WriteInt(w, len(fcr.NewValidProofOutputs))
	for _, sco := range fcr.NewValidProofOutputs {
		sco.MarshalSia(w)
	}
	encoding.WriteInt(w, len(fcr.NewMissedProofOutputs))
	for _, sco := range fcr.NewMissedProofOutputs {
		sco.MarshalSia(w)
	}
	_, err := w.Write(fcr.NewUnlockHash[:])
	return err
}

// MarshalSiaSize returns the encoded size of fcr.
func (fcr FileContractRevision) MarshalSiaSize() (size int) {
	size += len(fcr.ParentID)
	size += fcr.UnlockConditions.MarshalSiaSize()
	size += 8 // NewRevisionNumber
	size += 8 // NewFileSize
	size += len(fcr.NewFileMerkleRoot)
	size += 8 + 8 // NewWindowStart + NewWindowEnd
	size += 8
	for _, sco := range fcr.NewValidProofOutputs {
		size += sco.Value.MarshalSiaSize()
		size += len(sco.UnlockHash)
	}
	size += 8
	for _, sco := range fcr.NewMissedProofOutputs {
		size += sco.Value.MarshalSiaSize()
		size += len(sco.UnlockHash)
	}
	size += len(fcr.NewUnlockHash)
	return
}

// MarshalJSON marshals an id as a hex string.
func (fcid FileContractID) MarshalJSON() ([]byte, error) {
	return json.Marshal(fcid.String())
}

// String prints the id in hex.
func (fcid FileContractID) String() string {
	return fmt.Sprintf("%x", fcid[:])
}

// UnmarshalJSON decodes the json hex string of the id.
func (fcid *FileContractID) UnmarshalJSON(b []byte) error {
	return (*crypto.Hash)(fcid).UnmarshalJSON(b)
}

// MarshalJSON marshals an id as a hex string.
func (oid OutputID) MarshalJSON() ([]byte, error) {
	return json.Marshal(oid.String())
}

// String prints the id in hex.
func (oid OutputID) String() string {
	return fmt.Sprintf("%x", oid[:])
}

// UnmarshalJSON decodes the json hex string of the id.
func (oid *OutputID) UnmarshalJSON(b []byte) error {
	return (*crypto.Hash)(oid).UnmarshalJSON(b)
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (sci SiacoinInput) MarshalSia(w io.Writer) error {
	w.Write(sci.ParentID[:])
	return sci.UnlockConditions.MarshalSia(w)
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (sco SiacoinOutput) MarshalSia(w io.Writer) error {
	sco.Value.MarshalSia(w)
	_, err := w.Write(sco.UnlockHash[:])
	return err
}

// MarshalJSON marshals an id as a hex string.
func (scoid SiacoinOutputID) MarshalJSON() ([]byte, error) {
	return json.Marshal(scoid.String())
}

// String prints the id in hex.
func (scoid SiacoinOutputID) String() string {
	return fmt.Sprintf("%x", scoid[:])
}

// UnmarshalJSON decodes the json hex string of the id.
func (scoid *SiacoinOutputID) UnmarshalJSON(b []byte) error {
	return (*crypto.Hash)(scoid).UnmarshalJSON(b)
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (sfi SiafundInput) MarshalSia(w io.Writer) error {
	w.Write(sfi.ParentID[:])
	sfi.UnlockConditions.MarshalSia(w)
	_, err := w.Write(sfi.ClaimUnlockHash[:])
	return err
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (sfo SiafundOutput) MarshalSia(w io.Writer) error {
	sfo.Value.MarshalSia(w)
	w.Write(sfo.UnlockHash[:])
	return sfo.ClaimStart.MarshalSia(w)
}

// MarshalJSON marshals an id as a hex string.
func (sfoid SiafundOutputID) MarshalJSON() ([]byte, error) {
	return json.Marshal(sfoid.String())
}

// String prints the id in hex.
func (sfoid SiafundOutputID) String() string {
	return fmt.Sprintf("%x", sfoid[:])
}

// UnmarshalJSON decodes the json hex string of the id.
func (sfoid *SiafundOutputID) UnmarshalJSON(b []byte) error {
	return (*crypto.Hash)(sfoid).UnmarshalJSON(b)
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (spk SiaPublicKey) MarshalSia(w io.Writer) error {
	w.Write(spk.Algorithm[:])
	return encoding.WritePrefix(w, spk.Key)
}

// LoadString is the inverse of SiaPublicKey.String().
func (spk *SiaPublicKey) LoadString(s string) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return
	}
	var err error
	spk.Key, err = hex.DecodeString(parts[1])
	if err != nil {
		spk.Key = nil
		return
	}
	copy(spk.Algorithm[:], []byte(parts[0]))
}

// String defines how to print a SiaPublicKey - hex is used to keep things
// compact during logging. The key type prefix and lack of a checksum help to
// separate it from a sia address.
func (spk *SiaPublicKey) String() string {
	return spk.Algorithm.String() + ":" + fmt.Sprintf("%x", spk.Key)
}

// MarshalJSON marshals a specifier as a string.
func (s Specifier) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// String returns the specifier as a string, trimming any trailing zeros.
func (s Specifier) String() string {
	var i int
	for i = range s {
		if s[i] == 0 {
			break
		}
	}
	return string(s[:i])
}

// UnmarshalJSON decodes the json string of the specifier.
func (s *Specifier) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return err
	}
	copy(s[:], str)
	return nil
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (sp *StorageProof) MarshalSia(w io.Writer) error {
	w.Write(sp.ParentID[:])
	w.Write(sp.Segment[:])
	encoding.WriteInt(w, len(sp.HashSet))
	for i := range sp.HashSet {
		if _, err := w.Write(sp.HashSet[i][:]); err != nil {
			return err
		}
	}
	return nil
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (t Transaction) MarshalSia(w io.Writer) error {
	if build.DEBUG {
		// Sanity check: compare against the old encoding
		buf := new(bytes.Buffer)
		encoding.NewEncoder(buf).EncodeAll(
			t.SiacoinInputs,
			t.SiacoinOutputs,
			t.FileContracts,
			t.FileContractRevisions,
			t.StorageProofs,
			t.SiafundInputs,
			t.SiafundOutputs,
			t.MinerFees,
			t.ArbitraryData,
			t.TransactionSignatures,
		)
		w = sanityCheckWriter{w, buf}
	}

	t.marshalSiaNoSignatures(w)
	encoding.WriteInt(w, len((t.TransactionSignatures)))
	for i := range t.TransactionSignatures {
		err := t.TransactionSignatures[i].MarshalSia(w)
		if err != nil {
			return err
		}
	}
	return nil
}

// marshalSiaNoSignatures is a helper function for calculating certain hashes
// that do not include the transaction's signatures.
func (t Transaction) marshalSiaNoSignatures(w io.Writer) {
	encoding.WriteInt(w, len((t.SiacoinInputs)))
	for i := range t.SiacoinInputs {
		t.SiacoinInputs[i].MarshalSia(w)
	}
	encoding.WriteInt(w, len((t.SiacoinOutputs)))
	for i := range t.SiacoinOutputs {
		t.SiacoinOutputs[i].MarshalSia(w)
	}
	encoding.WriteInt(w, len((t.FileContracts)))
	for i := range t.FileContracts {
		t.FileContracts[i].MarshalSia(w)
	}
	encoding.WriteInt(w, len((t.FileContractRevisions)))
	for i := range t.FileContractRevisions {
		t.FileContractRevisions[i].MarshalSia(w)
	}
	encoding.WriteInt(w, len((t.StorageProofs)))
	for i := range t.StorageProofs {
		t.StorageProofs[i].MarshalSia(w)
	}
	encoding.WriteInt(w, len((t.SiafundInputs)))
	for i := range t.SiafundInputs {
		t.SiafundInputs[i].MarshalSia(w)
	}
	encoding.WriteInt(w, len((t.SiafundOutputs)))
	for i := range t.SiafundOutputs {
		t.SiafundOutputs[i].MarshalSia(w)
	}
	encoding.WriteInt(w, len((t.MinerFees)))
	for i := range t.MinerFees {
		t.MinerFees[i].MarshalSia(w)
	}
	encoding.WriteInt(w, len((t.ArbitraryData)))
	for i := range t.ArbitraryData {
		encoding.WritePrefix(w, t.ArbitraryData[i])
	}
}

// MarshalSiaSize returns the encoded size of t.
func (t Transaction) MarshalSiaSize() (size int) {
	size += 8
	for _, sci := range t.SiacoinInputs {
		size += len(sci.ParentID)
		size += sci.UnlockConditions.MarshalSiaSize()
	}
	size += 8
	for _, sco := range t.SiacoinOutputs {
		size += sco.Value.MarshalSiaSize()
		size += len(sco.UnlockHash)
	}
	size += 8
	for i := range t.FileContracts {
		size += t.FileContracts[i].MarshalSiaSize()
	}
	size += 8
	for i := range t.FileContractRevisions {
		size += t.FileContractRevisions[i].MarshalSiaSize()
	}
	size += 8
	for _, sp := range t.StorageProofs {
		size += len(sp.ParentID)
		size += len(sp.Segment)
		size += 8 + len(sp.HashSet)*crypto.HashSize
	}
	size += 8
	for _, sfi := range t.SiafundInputs {
		size += len(sfi.ParentID)
		size += len(sfi.ClaimUnlockHash)
		size += sfi.UnlockConditions.MarshalSiaSize()
	}
	size += 8
	for _, sfo := range t.SiafundOutputs {
		size += sfo.Value.MarshalSiaSize()
		size += len(sfo.UnlockHash)
		size += sfo.ClaimStart.MarshalSiaSize()
	}
	size += 8
	for i := range t.MinerFees {
		size += t.MinerFees[i].MarshalSiaSize()
	}
	size += 8
	for i := range t.ArbitraryData {
		size += 8 + len(t.ArbitraryData[i])
	}
	size += 8
	for _, ts := range t.TransactionSignatures {
		size += len(ts.ParentID)
		size += 8 // ts.PublicKeyIndex
		size += 8 // ts.Timelock
		size += ts.CoveredFields.MarshalSiaSize()
		size += 8 + len(ts.Signature)
	}

	// Sanity check against the slower method.
	if build.DEBUG {
		expectedSize := len(encoding.Marshal(t))
		if expectedSize != size {
			panic("Transaction size different from expected size.")
		}
	}
	return
}

// MarshalJSON marshals an id as a hex string.
func (tid TransactionID) MarshalJSON() ([]byte, error) {
	return json.Marshal(tid.String())
}

// String prints the id in hex.
func (tid TransactionID) String() string {
	return fmt.Sprintf("%x", tid[:])
}

// UnmarshalJSON decodes the json hex string of the id.
func (tid *TransactionID) UnmarshalJSON(b []byte) error {
	return (*crypto.Hash)(tid).UnmarshalJSON(b)
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (ts TransactionSignature) MarshalSia(w io.Writer) error {
	w.Write(ts.ParentID[:])
	encoding.WriteUint64(w, ts.PublicKeyIndex)
	encoding.WriteUint64(w, uint64(ts.Timelock))
	ts.CoveredFields.MarshalSia(w)
	return encoding.WritePrefix(w, ts.Signature)
}

// MarshalSia implements the encoding.SiaMarshaler interface.
func (uc UnlockConditions) MarshalSia(w io.Writer) error {
	encoding.WriteUint64(w, uint64(uc.Timelock))
	encoding.WriteInt(w, len(uc.PublicKeys))
	for _, spk := range uc.PublicKeys {
		spk.MarshalSia(w)
	}
	return encoding.WriteUint64(w, uc.SignaturesRequired)
}

// MarshalSiaSize returns the encoded size of uc.
func (uc UnlockConditions) MarshalSiaSize() (size int) {
	size += 8 // Timelock
	size += 8 // length prefix for PublicKeys
	for _, spk := range uc.PublicKeys {
		size += len(spk.Algorithm)
		size += 8 + len(spk.Key)
	}
	size += 8 // SignaturesRequired
	return
}

// MarshalJSON is implemented on the unlock hash to always produce a hex string
// upon marshalling.
func (uh UnlockHash) MarshalJSON() ([]byte, error) {
	return json.Marshal(uh.String())
}

// UnmarshalJSON is implemented on the unlock hash to recover an unlock hash
// that has been encoded to a hex string.
func (uh *UnlockHash) UnmarshalJSON(b []byte) error {
	// Check the length of b.
	if len(b) != crypto.HashSize*2+UnlockHashChecksumSize*2+2 && len(b) != crypto.HashSize*2+2 {
		return ErrUnlockHashWrongLen
	}
	return uh.LoadString(string(b[1 : len(b)-1]))
}

// String returns the hex representation of the unlock hash as a string - this
// includes a checksum.
func (uh UnlockHash) String() string {
	uhChecksum := crypto.HashObject(uh)
	return fmt.Sprintf("%x%x", uh[:], uhChecksum[:UnlockHashChecksumSize])
}

// LoadString loads a hex representation (including checksum) of an unlock hash
// into an unlock hash object. An error is returned if the string is invalid or
// fails the checksum.
func (uh *UnlockHash) LoadString(strUH string) error {
	// Check the length of strUH.
	if len(strUH) != crypto.HashSize*2+UnlockHashChecksumSize*2 {
		return ErrUnlockHashWrongLen
	}

	// Decode the unlock hash.
	var byteUnlockHash []byte
	var checksum []byte
	_, err := fmt.Sscanf(strUH[:crypto.HashSize*2], "%x", &byteUnlockHash)
	if err != nil {
		return err
	}

	// Decode and verify the checksum.
	_, err = fmt.Sscanf(strUH[crypto.HashSize*2:], "%x", &checksum)
	if err != nil {
		return err
	}
	expectedChecksum := crypto.HashBytes(byteUnlockHash)
	if !bytes.Equal(expectedChecksum[:UnlockHashChecksumSize], checksum) {
		return ErrInvalidUnlockHashChecksum
	}

	copy(uh[:], byteUnlockHash[:])
	return nil
}
