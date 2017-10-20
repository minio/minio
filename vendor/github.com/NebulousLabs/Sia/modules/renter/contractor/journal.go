package contractor

// The contractor achieves efficient persistence using a JSON transaction
// journal. It enables efficient ACID transactions on JSON objects.
//
// The journal represents a single JSON object, containing all of the
// contractor's persisted data. The object is serialized as an "initial
// object" followed by a series of update sets, one per line. Each update
// specifies a modification.
//
// During operation, the object is first loaded by reading the file and
// applying each update to the initial object. It is subsequently modified by
// appending update sets to the file, one per line. At any time, a
// "checkpoint" may be created, which clears the journal and starts over with
// a new initial object. This allows for compaction of the journal file.
//
// In the event of power failure or other serious disruption, the most recent
// update set may be only partially written. Partially written update sets are
// simply ignored when reading the journal.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/persist"
	"github.com/NebulousLabs/Sia/types"
)

var journalMeta = persist.Metadata{
	Header:  "Contractor Journal",
	Version: "1.1.1",
}

// A journal is a log of updates to a JSON object.
type journal struct {
	f        *os.File
	filename string
}

// update applies the updateSet atomically to j. It syncs the underlying file
// before returning.
func (j *journal) update(us updateSet) error {
	if err := json.NewEncoder(j.f).Encode(us); err != nil {
		return err
	}
	return j.f.Sync()
}

// Checkpoint refreshes the journal with a new initial object. It syncs the
// underlying file before returning.
func (j *journal) checkpoint(data contractorPersist) error {
	if build.DEBUG {
		// Sanity check - applying the updates to the initial object should
		// result in a contractorPersist that matches data.
		var data2 contractorPersist
		j2, err := openJournal(j.filename, &data2)
		if err != nil {
			panic("could not open journal for sanity check: " + err.Error())
		}
		for id, c := range data.CachedRevisions {
			if c2, ok := data2.CachedRevisions[id]; !ok {
				continue
			} else if !reflect.DeepEqual(c.Revision, c2.Revision) {
				panic("CachedRevision Revisions mismatch: " + fmt.Sprint(c.Revision, c2.Revision))
			} else if !reflect.DeepEqual(c.MerkleRoots, c2.MerkleRoots) && (c.MerkleRoots == nil) == (c2.MerkleRoots == nil) {
				panic("CachedRevision Merkle roots mismatch: " + fmt.Sprint(len(c.MerkleRoots), len(c2.MerkleRoots)))
			}
		}
		for id, c := range data.Contracts {
			if c2, ok := data2.Contracts[id]; !ok {
				continue
			} else if !reflect.DeepEqual(c.LastRevisionTxn, c2.LastRevisionTxn) {
				panic("Contract Txns mismatch: " + fmt.Sprint(c.LastRevisionTxn, c2.LastRevisionTxn))
			} else if !reflect.DeepEqual(c.MerkleRoots, c2.MerkleRoots) && (c.MerkleRoots == nil) == (c2.MerkleRoots == nil) {
				panic("Contract Merkle roots mismatch: " + fmt.Sprint(len(c.MerkleRoots), len(c2.MerkleRoots)))
			}
		}
		j2.Close()
	}

	// Write to a new temp file.
	tmp, err := os.Create(j.filename + "_tmp")
	if err != nil {
		return err
	}
	enc := json.NewEncoder(tmp)
	if err := enc.Encode(journalMeta); err != nil {
		return err
	}
	if err := enc.Encode(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}

	// Atomically replace the old file with the new one.
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := j.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), j.filename); err != nil {
		return err
	}

	// Reopen the journal.
	j.f, err = os.OpenFile(j.filename, os.O_RDWR|os.O_APPEND, 0)
	return err
}

// Close closes the underlying file.
func (j *journal) Close() error {
	return j.f.Close()
}

// newJournal creates a new journal, using data as the initial object.
func newJournal(filename string, data contractorPersist) (*journal, error) {
	// safely create the journal
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	enc := json.NewEncoder(f)
	if err := enc.Encode(journalMeta); err != nil {
		return nil, err
	}
	if err := enc.Encode(data); err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}

	return &journal{f: f, filename: filename}, nil
}

// openJournal opens the supplied journal and decodes the reconstructed
// contractorPersist into data.
func openJournal(filename string, data *contractorPersist) (*journal, error) {
	// Open file handle for reading and writing.
	f, err := os.OpenFile(filename, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}

	// Decode the metadata.
	dec := json.NewDecoder(f)
	var meta persist.Metadata
	if err = dec.Decode(&meta); err != nil {
		return nil, err
	} else if meta.Header != journalMeta.Header {
		return nil, fmt.Errorf("expected header %q, got %q", journalMeta.Header, meta.Header)
	} else if meta.Version != journalMeta.Version {
		return nil, fmt.Errorf("journal version (%s) is incompatible with the current version (%s)", meta.Version, journalMeta.Version)
	}

	// Decode the initial object.
	if err = dec.Decode(data); err != nil {
		return nil, err
	}

	// Make sure all maps are properly initialized.
	if data.CachedRevisions == nil {
		data.CachedRevisions = map[string]cachedRevision{}
	}
	if data.Contracts == nil {
		data.Contracts = map[string]modules.RenterContract{}
	}
	if data.RenewedIDs == nil {
		data.RenewedIDs = map[string]string{}
	}

	// Decode each set of updates and apply them to data.
	for {
		var set updateSet
		if err = dec.Decode(&set); err == io.EOF || err == io.ErrUnexpectedEOF {
			// unexpected EOF means the last update was corrupted; skip it
			break
		} else if err != nil {
			// skip corrupted update sets
			continue
		}
		for _, u := range set {
			u.apply(data)
		}
	}

	return &journal{
		f:        f,
		filename: filename,
	}, nil
}

type journalUpdate interface {
	apply(*contractorPersist)
}

type marshaledUpdate struct {
	Type     string          `json:"type"`
	Data     json.RawMessage `json:"data"`
	Checksum crypto.Hash     `json:"checksum"`
}

type updateSet []journalUpdate

// MarshalJSON marshals a set of journalUpdates as an array of
// marshaledUpdates.
func (set updateSet) MarshalJSON() ([]byte, error) {
	marshaledSet := make([]marshaledUpdate, len(set))
	for i, u := range set {
		data, err := json.Marshal(u)
		if err != nil {
			build.Critical("failed to marshal known type:", err)
		}
		marshaledSet[i].Data = data
		marshaledSet[i].Checksum = crypto.HashBytes(data)
		switch u.(type) {
		case updateUploadRevision:
			marshaledSet[i].Type = "uploadRevision"
		case updateDownloadRevision:
			marshaledSet[i].Type = "downloadRevision"
		case updateCachedUploadRevision:
			marshaledSet[i].Type = "cachedUploadRevision"
		case updateCachedDownloadRevision:
			marshaledSet[i].Type = "cachedDownloadRevision"
		}
	}
	return json.Marshal(marshaledSet)
}

// UnmarshalJSON unmarshals an array of marshaledUpdates as a set of
// journalUpdates.
func (set *updateSet) UnmarshalJSON(b []byte) error {
	var marshaledSet []marshaledUpdate
	if err := json.Unmarshal(b, &marshaledSet); err != nil {
		return err
	}
	for _, u := range marshaledSet {
		if crypto.HashBytes(u.Data) != u.Checksum {
			return errors.New("bad checksum")
		}
		var err error
		switch u.Type {
		case "uploadRevision":
			var ur updateUploadRevision
			err = json.Unmarshal(u.Data, &ur)
			*set = append(*set, ur)
		case "downloadRevision":
			var dr updateDownloadRevision
			err = json.Unmarshal(u.Data, &dr)
			*set = append(*set, dr)
		case "cachedUploadRevision":
			var cur updateCachedUploadRevision
			err = json.Unmarshal(u.Data, &cur)
			*set = append(*set, cur)
		case "cachedDownloadRevision":
			var cdr updateCachedDownloadRevision
			err = json.Unmarshal(u.Data, &cdr)
			*set = append(*set, cdr)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// updateUploadRevision is a journalUpdate that records the new data
// associated with uploading a sector to a host.
type updateUploadRevision struct {
	NewRevisionTxn     types.Transaction `json:"newrevisiontxn"`
	NewSectorRoot      crypto.Hash       `json:"newsectorroot"`
	NewSectorIndex     int               `json:"newsectorindex"`
	NewUploadSpending  types.Currency    `json:"newuploadspending"`
	NewStorageSpending types.Currency    `json:"newstoragespending"`
}

// apply sets the LastRevision, LastRevisionTxn, UploadSpending, and
// DownloadSpending fields of the contract being revised. It also adds the new
// Merkle root to the contract's Merkle root set.
func (u updateUploadRevision) apply(data *contractorPersist) {
	if len(u.NewRevisionTxn.FileContractRevisions) == 0 {
		build.Critical("updateUploadRevision is missing its FileContractRevision")
		return
	}

	rev := u.NewRevisionTxn.FileContractRevisions[0]
	c := data.Contracts[rev.ParentID.String()]
	c.LastRevisionTxn = u.NewRevisionTxn
	c.LastRevision = rev

	if u.NewSectorIndex == len(c.MerkleRoots) {
		c.MerkleRoots = append(c.MerkleRoots, u.NewSectorRoot)
	} else if u.NewSectorIndex < len(c.MerkleRoots) {
		c.MerkleRoots[u.NewSectorIndex] = u.NewSectorRoot
	} else {
		// Shouldn't happen. TODO: Correctly handle error.
	}

	c.UploadSpending = u.NewUploadSpending
	c.StorageSpending = u.NewStorageSpending
	data.Contracts[rev.ParentID.String()] = c
}

// updateUploadRevision is a journalUpdate that records the new data
// associated with downloading a sector from a host.
type updateDownloadRevision struct {
	NewRevisionTxn      types.Transaction `json:"newrevisiontxn"`
	NewDownloadSpending types.Currency    `json:"newdownloadspending"`
}

// apply sets the LastRevision, LastRevisionTxn, and DownloadSpending fields
// of the contract being revised.
func (u updateDownloadRevision) apply(data *contractorPersist) {
	if len(u.NewRevisionTxn.FileContractRevisions) == 0 {
		build.Critical("updateDownloadRevision is missing its FileContractRevision")
		return
	}
	rev := u.NewRevisionTxn.FileContractRevisions[0]
	c := data.Contracts[rev.ParentID.String()]
	c.LastRevisionTxn = u.NewRevisionTxn
	c.LastRevision = rev
	c.DownloadSpending = u.NewDownloadSpending
	data.Contracts[rev.ParentID.String()] = c
}

// updateCachedUploadRevision is a journalUpdate that records the unsigned
// revision sent to the host during a sector upload, along with the Merkle
// root of the new sector.
type updateCachedUploadRevision struct {
	Revision    types.FileContractRevision `json:"revision"`
	SectorRoot  crypto.Hash                `json:"sectorroot"`
	SectorIndex int                        `json:"sectorindex"`
}

// apply sets the Revision field of the cachedRevision associated with the
// contract being revised, as well as the Merkle root of the new sector.
func (u updateCachedUploadRevision) apply(data *contractorPersist) {
	c := data.CachedRevisions[u.Revision.ParentID.String()]
	c.Revision = u.Revision
	if u.SectorIndex == len(c.MerkleRoots) {
		c.MerkleRoots = append(c.MerkleRoots, u.SectorRoot)
	} else if u.SectorIndex < len(c.MerkleRoots) {
		c.MerkleRoots[u.SectorIndex] = u.SectorRoot
	} else {
		// Shouldn't happen. TODO: Add correct error handling.
	}
	data.CachedRevisions[u.Revision.ParentID.String()] = c
}

// updateCachedDownloadRevision is a journalUpdate that records the unsigned
// revision sent to the host during a sector download.
type updateCachedDownloadRevision struct {
	Revision types.FileContractRevision `json:"revision"`
}

// apply sets the Revision field of the cachedRevision associated with the
// contract being revised.
func (u updateCachedDownloadRevision) apply(data *contractorPersist) {
	c := data.CachedRevisions[u.Revision.ParentID.String()]
	c.Revision = u.Revision
	data.CachedRevisions[u.Revision.ParentID.String()] = c
}
