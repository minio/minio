package data

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/HouzuoGuo/tiedot/dberr"
)

func TestPartitionDocCRUD(t *testing.T) {
	colPath := "/tmp/tiedot_test_col"
	htPath := "/tmp/tiedot_test_ht"
	os.Remove(colPath)
	os.Remove(htPath)
	defer os.Remove(colPath)
	defer os.Remove(htPath)
	part, err := OpenPartition(colPath, htPath)
	if err != nil {
		t.Fatal(err)
	}
	// Insert & read
	if _, err = part.Insert(1, []byte("1")); err != nil {
		t.Fatal(err)
	}
	if _, err = part.Insert(2, []byte("2")); err != nil {
		t.Fatal(err)
	}
	if readback, err := part.Read(1); err != nil || string(readback) != "1 " {
		t.Fatal(err, readback)
	}
	if readback, err := part.Read(2); err != nil || string(readback) != "2 " {
		t.Fatal(err, readback)
	}
	// Update & read
	if err = part.Update(1, []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if err := part.Update(1234, []byte("abcdef")); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("Did not error")
	}
	if readback, err := part.Read(1); err != nil || string(readback) != "abcdef      " {
		t.Fatal(err, readback)
	}
	// Delete & read
	if err = part.Delete(1); err != nil {
		t.Fatal(err)
	}
	if _, err = part.Read(1); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("Did not error")
	}
	if err = part.Delete(123); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("Did not error")
	}
	// Lock & unlock
	if err = part.LockUpdate(123); err != nil {
		t.Fatal(err)
	}
	if err = part.LockUpdate(123); dberr.Type(err) != dberr.ErrorDocLocked {
		t.Fatal("Did not error")
	}
	part.UnlockUpdate(123)
	if err = part.LockUpdate(123); err != nil {
		t.Fatal(err)
	}
	// Foreach
	part.ForEachDoc(0, 1, func(id int, doc []byte) bool {
		if id != 2 || string(doc) != "2 " {
			t.Fatal("ID 2 should be the only remaining document")
		}
		return true
	})
	// Finish up
	if err = part.Clear(); err != nil {
		t.Fatal(err)
	}
	if err = part.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestApproxDocCount(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	colPath := "/tmp/tiedot_test_col"
	htPath := "/tmp/tiedot_test_ht"
	os.Remove(colPath)
	os.Remove(htPath)
	defer os.Remove(colPath)
	defer os.Remove(htPath)
	part, err := OpenPartition(colPath, htPath)
	if err != nil {
		t.Fatal(err)
	}
	defer part.Close()
	// Insert 100 documents
	for i := 0; i < 100; i++ {
		if _, err = part.Insert(rand.Int(), []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("ApproxDocCount", part.ApproxDocCount())
	if part.ApproxDocCount() < 10 || part.ApproxDocCount() > 300 {
		t.Fatal("Approximate is way off", part.ApproxDocCount())
	}
	// Insert 900 documents
	for i := 0; i < 900; i++ {
		if _, err = part.Insert(rand.Int(), []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("ApproxDocCount", part.ApproxDocCount())
	if part.ApproxDocCount() < 500 || part.ApproxDocCount() > 1500 {
		t.Fatal("Approximate is way off", part.ApproxDocCount())
	}
	// Insert another 2000 documents
	for i := 0; i < 2000; i++ {
		if _, err = part.Insert(rand.Int(), []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("ApproxDocCount", part.ApproxDocCount())
	if part.ApproxDocCount() < 2000 || part.ApproxDocCount() > 4000 {
		t.Fatal("Approximate is way off", part.ApproxDocCount())
	}
	// See how fast doc count is
	start := time.Now().UnixNano()
	for i := 0; i < 1000; i++ {
		part.ApproxDocCount()
	}
	timediff := time.Now().UnixNano() - start
	t.Log("It took", timediff/1000000, "milliseconds")
	if timediff/1000000 > 3500 {
		t.Fatal("Algorithm is way too slow")
	}
}
