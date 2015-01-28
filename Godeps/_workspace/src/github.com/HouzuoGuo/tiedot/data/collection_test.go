package data

import (
	"os"
	"strings"
	"testing"

	"github.com/HouzuoGuo/tiedot/dberr"
)

func TestInsertRead(t *testing.T) {
	tmp := "/tmp/tiedot_test_col"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCollection(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234")}
	ids := [2]int{}
	if ids[0], err = col.Insert(docs[0]); ids[0] != 0 || err != nil {
		t.Fatalf("Failed to insert: %d %v", ids[0], err)
	}
	if ids[1], err = col.Insert(docs[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if doc0 := col.Read(ids[0]); doc0 == nil || strings.TrimSpace(string(doc0)) != string(docs[0]) {
		t.Fatal("Failed to read", doc0)
	}
	if doc1 := col.Read(ids[1]); doc1 == nil || strings.TrimSpace(string(doc1)) != string(docs[1]) {
		t.Fatalf("Failed to read")
	}
	// it shall not panic
	col.Read(col.Size)
}

func TestInsertUpdateRead(t *testing.T) {
	tmp := "/tmp/tiedot_test_col"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCollection(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234")}
	ids := [2]int{}
	if ids[0], err = col.Insert(docs[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(docs[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	updated := [2]int{}
	if updated[0], err = col.Update(ids[0], []byte("abcdef")); err != nil || updated[0] != ids[0] {
		t.Fatalf("Failed to update: %v", err)
	}
	if updated[1], err = col.Update(ids[1], []byte("longlonglonglonglonglonglong")); err != nil || updated[1] == ids[1] {
		t.Fatalf("Failed to update: %v", err)
	}
	if doc0 := col.Read(updated[0]); doc0 == nil || strings.TrimSpace(string(doc0)) != "abcdef" {
		t.Fatalf("Failed to read")
	}
	if doc1 := col.Read(updated[1]); doc1 == nil || strings.TrimSpace(string(doc1)) != "longlonglonglonglonglonglong" {
		t.Fatalf("Failed to read")
	}
	// it shall not panic
	col.Update(col.Size, []byte("abcdef"))
}

func TestInsertDeleteRead(t *testing.T) {
	tmp := "/tmp/tiedot_test_col"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCollection(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234"),
		[]byte("2345")}
	ids := [3]int{}
	if ids[0], err = col.Insert(docs[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(docs[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[2], err = col.Insert(docs[2]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if doc0 := col.Read(ids[0]); doc0 == nil || strings.TrimSpace(string(doc0)) != string(docs[0]) {
		t.Fatalf("Failed to read")
	}
	if err = col.Delete(ids[1]); err != nil {
		t.Fatal(err)
	}
	if doc1 := col.Read(ids[1]); doc1 != nil {
		t.Fatalf("Did not delete")
	}
	if doc2 := col.Read(ids[2]); doc2 == nil || strings.TrimSpace(string(doc2)) != string(docs[2]) {
		t.Fatalf("Failed to read")
	}
	// it shall not panic
	if err = col.Delete(col.Size); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("did not error")
	}
}

func TestInsertReadAll(t *testing.T) {
	tmp := "/tmp/tiedot_test_col"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCollection(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	var ids [5]int
	ids[0], err = col.Insert([]byte("abc"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	ids[1], err = col.Insert([]byte("abc"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	ids[2], err = col.Insert([]byte("abc"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	ids[3], err = col.Insert([]byte("abc"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	ids[4], err = col.Insert([]byte("abc"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	successfullyRead := 0
	t.Log(ids)
	col.ForEachDoc(func(_ int, _ []byte) bool {
		successfullyRead++
		return true
	})
	if successfullyRead != 5 {
		t.Fatalf("Should have read 5 documents, but only got %d", successfullyRead)
	}
	successfullyRead = 0
	// intentionally corrupt two docuemnts
	col.Buf[ids[4]] = 3     // corrupted validity
	col.Buf[ids[2]+1] = 255 // corrupted room
	col.ForEachDoc(func(_ int, _ []byte) bool {
		successfullyRead++
		return true
	})
	if successfullyRead != 3 {
		t.Fatalf("Should have read 3 documents, but %d are recovered", successfullyRead)
	}
}

func TestCollectionGrowAndOutOfBoundAccess(t *testing.T) {
	tmp := "/tmp/tiedot_test_col"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCollection(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	// Insert several documents
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234"),
		[]byte("2345")}
	if _, err = col.Insert(docs[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if _, err = col.Insert(docs[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if _, err = col.Insert(docs[2]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	// Test UsedSize
	calculatedUsedSize := (DOC_HEADER + 3*2) + (DOC_HEADER+4*2)*2
	if col.Used != calculatedUsedSize {
		t.Fatalf("Invalid UsedSize")
	}
	// Read invalid location
	if doc := col.Read(1); doc != nil {
		t.Fatalf("Read invalid location")
	}
	if doc := col.Read(col.Used); doc != nil {
		t.Fatalf("Read invalid location")
	}
	if doc := col.Read(col.Size); doc != nil {
		t.Fatalf("Read invalid location")
	}
	if doc := col.Read(999999999); doc != nil {
		t.Fatalf("Read invalid location")
	}
	// Update invalid location
	if _, err := col.Update(1, []byte{}); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatalf("Update invalid location")
	}
	if _, err := col.Update(col.Used, []byte{}); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatalf("Update invalid location")
	}
	if _, err := col.Update(col.Size, []byte{}); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatalf("Update invalid location")
	}
	if _, err := col.Update(999999999, []byte{}); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatalf("Update invalid location")
	}
	// Delete invalid location
	if err = col.Delete(1); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("did not error")
	}
	if err = col.Delete(col.Used); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("did not error")
	}
	if err = col.Delete(col.Size); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("did not error")
	}
	if err = col.Delete(999999999); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("did not error")
	}
	// Insert - not enough room
	count := 0
	for i := 0; i < COL_FILE_GROWTH; i += DOC_MAX_ROOM {
		if _, err := col.Insert(make([]byte, DOC_MAX_ROOM/2)); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if _, err := col.Insert(make([]byte, DOC_MAX_ROOM/2)); err != nil {
		t.Fatal(err)
	}
	count++
	calculatedUsedSize += count * (DOC_HEADER + DOC_MAX_ROOM)
	if col.Used != calculatedUsedSize {
		t.Fatalf("Wrong UsedSize %d %d", col.Used, calculatedUsedSize)
	}
	if col.Size != COL_FILE_GROWTH+col.Growth {
		t.Fatalf("Size changed?! %d %d %d", col.Size, COL_FILE_GROWTH, col.Growth)
	}
	if err = col.Close(); err != nil {
		t.Fatal(err)
	}
}
