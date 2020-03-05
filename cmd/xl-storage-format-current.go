package cmd

import (
	"strings"
	"time"
)

const (
	xlMetaVersion = "2.0.0"
)

// The []journal contains all the different versions of the object.
//
// This array can have 3 kinds of objects:
//
// ``object``: If the object is uploaded the usual way: putobject, multipart-put, copyobject
//
// ``link``: If the object was copied(restored) from the older version to the latest using the copyobject API.
//           This an optimization so that when an older version object is restored we don't copy the data,
//           but just create metadata in the xl.json link and object will share the same backend data directory.
//
// ``delete``: This is the delete-marker

// The last element of this array is the latest. We will support versioning on newly created buckets.
// i.e we don't support versioning on existing objects.

// backend directory tree structure:

// disk1/
// └── bucket
//     └── object
//         ├── a192c1d5-9bd5-41fd-9a90-ab10e165398d
//         │   └── part.1
//         ├── c06e0436-f813-447e-ae5e-f2564df9dfd4
//         │   └── part.1
//         ├── df433928-2dcf-47b1-a786-43efa0f6b424
//         │   └── part.1
//         └── xl.json

// ErasureAlgo types of erasure algorithm
type ErasureAlgo int

// Supported erasure algorithms
const (
	ReedSolomon ErasureAlgo = iota
)

// ChecksumAlgo types of checksum algorithm
type ChecksumAlgo int

// Supported highway hash streaming
const (
	HighwayHashStreaming ChecksumAlgo = iota
)

// Journal represents different types of object version states
type Journal int

// Supported journal types
const (
	JournalDelete Journal = iota
	JournalLink
	JournalObject
)

type xlMetaV2DeleteMarker struct {
	VersionID string `json:"id"`
	ModTime   int64  `json:"modTime"`
}

func (d xlMetaV2DeleteMarker) Valid() bool {
	return d.VersionID != "" && d.ModTime > 0
}

type xlMetaV2Object struct {
	VersionID string `json:"id"`
	Data      struct {
		Dir     string `json:"dir"`
		Erasure struct {
			Algorithm    ErasureAlgo `json:"algorithm"`
			Data         int         `json:"data"`
			Parity       int         `json:"parity"`
			BlockSize    int         `json:"blockSize"`
			Index        int         `json:"index"`
			Distribution []int       `json:"distribution"`
			Checksum     struct {
				Algorithm ChecksumAlgo `json:"algorithm"`
			} `json:"checksum"`
		} `json:"erasure"`
		Parts struct {
			Sizes       []int64 `json:"sizes"`
			ActualSizes []int64 `json:"actualSizes"`
		} `json:"parts"`
	} `json:"data"`
	Stat struct {
		Size    int64 `json:"size"`
		ModTime int64 `json:"modTime"`
	} `json:"stat"`
	Meta struct {
		Sys  map[string]string `json:"sys"`
		User map[string]string `json:"user"`
	} `json:"meta"`
}

func (o xlMetaV2Object) Valid() bool {
	if o.VersionID == "" {
		return false
	}
	return isXLMetaErasureInfoValid(o.Data.Erasure.Data, o.Data.Erasure.Parity)
}

type xlMetaV2Link xlMetaV2Object

func (l xlMetaV2Link) Valid() bool {
	return xlMetaV2Object(l).Valid()
}

type xlMetaV2JournalEntry struct {
	Type         Journal               `json:"type"`
	DeleteMarker *xlMetaV2DeleteMarker `json:"delete,omitempty"`
	Object       *xlMetaV2Object       `json:"object,omitempty"`
	Link         *xlMetaV2Link         `json:"link,omitempty"`
}

func (j xlMetaV2JournalEntry) Valid() bool {
	switch j.Type {
	default:
		return false
	case JournalDelete:
		return j.DeleteMarker.Valid()
	case JournalLink:
		return j.Link.Valid()
	case JournalObject:
		return j.Object.Valid()
	}
}

type xlMetaV2 struct {
	Version string `json:"version"` // Version of the current `xl.json`.
	Format  string `json:"format"`  // Format of the current `xl.json`.
	XL      struct {
		Journal []xlMetaV2JournalEntry `json:"journal"`
	} `json:"xl"`
}

func newXLMetaV2Object(object string, versionID string, dataBlocks, parityBlocks int) *xlMetaV2Object {
	obj := &xlMetaV2Object{}
	obj.VersionID = versionID
	obj.Data.Erasure.Data = dataBlocks
	obj.Data.Erasure.Parity = parityBlocks
	obj.Data.Erasure.BlockSize = blockSizeV1
	obj.Data.Erasure.Algorithm = ReedSolomon
	obj.Data.Erasure.Checksum.Algorithm = HighwayHashStreaming
	obj.Data.Erasure.Distribution = hashOrder(object, dataBlocks+parityBlocks)
	obj.Meta.Sys = map[string]string{}
	obj.Meta.User = map[string]string{}
	return obj
}

func newXLMetaV2JournalDelete(object string, versionID string) xlMetaV2JournalEntry {
	return xlMetaV2JournalEntry{
		Type: JournalDelete,
		DeleteMarker: &xlMetaV2DeleteMarker{
			VersionID: versionID,
			ModTime:   UTCNow().Unix(),
		},
	}
}

func newXLMetaV2JournalLink(object string, versionID string, dataBlocks, parityBlocks int) xlMetaV2JournalEntry {
	link := xlMetaV2Link(*newXLMetaV2Object(object, versionID, dataBlocks, parityBlocks))
	journal := xlMetaV2JournalEntry{
		Type: JournalLink,
		Link: &link,
	}
	return journal
}

func newXLMetaV2JournalObject(object string, versionID string, dataBlocks, parityBlocks int) xlMetaV2JournalEntry {
	journal := xlMetaV2JournalEntry{
		Type:   JournalObject,
		Object: newXLMetaV2Object(object, versionID, dataBlocks, parityBlocks),
	}
	return journal
}

func newXLMetaV2(object string, versionID string, dataBlocks, parityBlocks int) xlMetaV2 {
	xlMeta := xlMetaV2{}
	xlMeta.Format = xlMetaFormat
	xlMeta.Version = xlMetaVersion
	xlMeta.XL.Journal = append(xlMeta.XL.Journal, newXLMetaV2JournalObject(object, versionID, dataBlocks, parityBlocks))
	return xlMeta
}

// Valid - tells us if the format is sane by validating
// format version string, and all journal entries.
func (m xlMetaV2) Valid() bool {
	if !isXLMetaFormatValid(m.Version, m.Format) {
		return false
	}
	for _, journal := range m.XL.Journal {
		if !journal.Valid() {
			return false
		}
	}
	return true
}

func (m xlMetaV2) ToFileInfo(volume, path, versionID string) FileInfo {
	fi := FileInfo{
		Volume:    volume,
		Name:      path,
		VersionID: versionID,
		Metadata:  make(map[string]string),
	}
	for _, journal := range m.XL.Journal {
		var oj xlMetaV2Object
		switch journal.Type {
		case JournalLink:
			if journal.Link.VersionID != versionID {
				continue
			}
			oj = xlMetaV2Object((*journal.Link))
		case JournalObject:
			if journal.Object.VersionID != versionID {
				continue
			}
			oj = *journal.Object
		}
		fi.Size = oj.Stat.Size
		fi.ModTime = time.Unix(oj.Stat.ModTime, 0)
		for k, v := range oj.Meta.User {
			fi.Metadata[k] = v
		}
		for k, v := range oj.Meta.Sys {
			if strings.HasPrefix(strings.ToLower(k), strings.ToLower(ReservedMetadataPrefix)) {
				fi.Metadata[k] = v
			}
		}
		fi.Parts = make([]ObjectPartInfo, len(oj.Data.Parts.Sizes))
		for i := range oj.Data.Parts.Sizes {
			fi.Parts[i].Number = i + 1
			fi.Parts[i].Size = oj.Data.Parts.Sizes[i]
			fi.Parts[i].ActualSize = oj.Data.Parts.ActualSizes[i]
		}
	}
	return fi
}
