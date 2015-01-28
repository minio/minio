/*
Collection data file contains document data. Every document has a binary header and UTF-8 text content.
Documents are inserted one after another, and occupies 2x original document size to leave room for future updates.
Deleted documents are marked as deleted and the space is irrecoverable until a "scrub" action (in DB logic) is carried out.
When update takes place, the new document may overwrite original document if there is enough space, otherwise the
original document is marked as deleted and the updated document is inserted as a new document.
*/
package data

import (
	"encoding/binary"

	"github.com/HouzuoGuo/tiedot/dberr"
)

const (
	COL_FILE_GROWTH = 32 * 1048576 // Collection file initial size & size growth (32 MBytes)
	DOC_MAX_ROOM    = 2 * 1048576  // Max document size (2 MBytes)
	DOC_HEADER      = 1 + 10       // Document header size - validity (single byte), document room (int 10 bytes)
	// Pre-compiled document padding (128 spaces)
	PADDING     = "                                                                                                                                "
	LEN_PADDING = len(PADDING)
)

// Collection file contains document headers and document text data.
type Collection struct {
	*DataFile
}

// Open a collection file.
func OpenCollection(path string) (col *Collection, err error) {
	col = new(Collection)
	col.DataFile, err = OpenDataFile(path, COL_FILE_GROWTH)
	return
}

// Find and retrieve a document by ID (physical document location). Return value is a copy of the document.
func (col *Collection) Read(id int) []byte {
	if id < 0 || id > col.Used-DOC_HEADER || col.Buf[id] != 1 {
		return nil
	} else if room, _ := binary.Varint(col.Buf[id+1 : id+11]); room > DOC_MAX_ROOM {
		return nil
	} else if docEnd := id + DOC_HEADER + int(room); docEnd >= col.Size {
		return nil
	} else {
		docCopy := make([]byte, room)
		copy(docCopy, col.Buf[id+DOC_HEADER:docEnd])
		return docCopy
	}
}

// Insert a new document, return the new document ID.
func (col *Collection) Insert(data []byte) (id int, err error) {
	room := len(data) << 1
	if room > DOC_MAX_ROOM {
		return 0, dberr.New(dberr.ErrorDocTooLarge, DOC_MAX_ROOM, room)
	}
	id = col.Used
	docSize := DOC_HEADER + room
	if err = col.EnsureSize(docSize); err != nil {
		return
	}
	col.Used += docSize
	// Write validity, room, document data and padding
	col.Buf[id] = 1
	binary.PutVarint(col.Buf[id+1:id+11], int64(room))
	copy(col.Buf[id+DOC_HEADER:col.Used], data)
	for padding := id + DOC_HEADER + len(data); padding < col.Used; padding += LEN_PADDING {
		copySize := LEN_PADDING
		if padding+LEN_PADDING >= col.Used {
			copySize = col.Used - padding
		}
		copy(col.Buf[padding:padding+copySize], PADDING)
	}
	return
}

// Overwrite or re-insert a document, return the new document ID if re-inserted.
func (col *Collection) Update(id int, data []byte) (newID int, err error) {
	dataLen := len(data)
	if dataLen > DOC_MAX_ROOM {
		return 0, dberr.New(dberr.ErrorDocTooLarge, DOC_MAX_ROOM, dataLen)
	}
	if id < 0 || id >= col.Used-DOC_HEADER || col.Buf[id] != 1 {
		return 0, dberr.New(dberr.ErrorNoDoc, id)
	}
	currentDocRoom, _ := binary.Varint(col.Buf[id+1 : id+11])
	if currentDocRoom > DOC_MAX_ROOM {
		return 0, dberr.New(dberr.ErrorNoDoc, id)
	}
	if docEnd := id + DOC_HEADER + int(currentDocRoom); docEnd >= col.Size {
		return 0, dberr.New(dberr.ErrorNoDoc, id)
	}
	if dataLen <= int(currentDocRoom) {
		padding := id + DOC_HEADER + len(data)
		paddingEnd := id + DOC_HEADER + int(currentDocRoom)
		// Overwrite data and then overwrite padding
		copy(col.Buf[id+DOC_HEADER:padding], data)
		for ; padding < paddingEnd; padding += LEN_PADDING {
			copySize := LEN_PADDING
			if padding+LEN_PADDING >= paddingEnd {
				copySize = paddingEnd - padding
			}
			copy(col.Buf[padding:padding+copySize], PADDING)
		}
		return id, nil
	} else {
		// No enough room - re-insert the document
		col.Delete(id)
		return col.Insert(data)
	}
}

// Delete a document by ID.
func (col *Collection) Delete(id int) error {

	if id < 0 || id > col.Used-DOC_HEADER || col.Buf[id] != 1 {
		return dberr.New(dberr.ErrorNoDoc, id)
	}

	if col.Buf[id] == 1 {
		col.Buf[id] = 0
	}

	return nil
}

// Run the function on every document; stop when the function returns false.
func (col *Collection) ForEachDoc(fun func(id int, doc []byte) bool) {
	for id := 0; id < col.Used-DOC_HEADER && id >= 0; {
		validity := col.Buf[id]
		room, _ := binary.Varint(col.Buf[id+1 : id+11])
		docEnd := id + DOC_HEADER + int(room)
		if (validity == 0 || validity == 1) && room <= DOC_MAX_ROOM && docEnd > 0 && docEnd <= col.Used {
			if validity == 1 && !fun(id, col.Buf[id+DOC_HEADER:docEnd]) {
				break
			}
			id = docEnd
		} else {
			// Corrupted document - move on
			id++
		}
	}
}
