/*
Hash table file contains binary content; it implements a static hash table made of hash buckets and integer entries.
Every bucket has a fixed number of entries. When a bucket becomes full, a new bucket is chained to it in order to store
more entries. Every entry has an integer key and value.
An entry key may have multiple values assigned to it, however the combination of entry key and value must be unique
across the entire hash table.
*/
package data

import (
	"encoding/binary"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"sync"
)

const (
	HT_FILE_GROWTH  = 32 * 1048576                          // Hash table file initial size & file growth
	ENTRY_SIZE      = 1 + 10 + 10                           // Hash entry size: validity (single byte), key (int 10 bytes), value (int 10 bytes)
	BUCKET_HEADER   = 10                                    // Bucket header size: next chained bucket number (int 10 bytes)
	PER_BUCKET      = 16                                    // Entries per bucket
	HASH_BITS       = 16                                    // Number of hash key bits
	BUCKET_SIZE     = BUCKET_HEADER + PER_BUCKET*ENTRY_SIZE // Size of a bucket
	INITIAL_BUCKETS = 65536                                 // Initial number of buckets == 2 ^ HASH_BITS
)

// Hash table file is a binary file containing buckets of hash entries.
type HashTable struct {
	*DataFile
	numBuckets int
	Lock       *sync.RWMutex
}

// Smear the integer entry key and return the portion (first HASH_BITS bytes) used for allocating the entry.
func HashKey(key int) int {
	/*
		tiedot should be compiled/run on x86-64 systems.
		If you decide to compile tiedot on 32-bit systems, the following integer-smear algorithm will cause compilation failure
		due to 32-bit interger overflow; therefore you must modify the algorithm.
		Do not remove the integer-smear process, and remember to run test cases to verify your mods.
	*/
	// ========== Integer-smear start =======
	key = key ^ (key >> 4)
	key = (key ^ 0xdeadbeef) + (key << 5)
	key = key ^ (key >> 11)
	// ========== Integer-smear end =========
	return key & ((1 << HASH_BITS) - 1) // Do not modify this line
}

// Open a hash table file.
func OpenHashTable(path string) (ht *HashTable, err error) {
	ht = &HashTable{Lock: new(sync.RWMutex)}
	if ht.DataFile, err = OpenDataFile(path, HT_FILE_GROWTH); err != nil {
		return
	}
	ht.calculateNumBuckets()
	return
}

// Follow the longest bucket chain to calculate total number of buckets, hence the "used size" of hash table file.
func (ht *HashTable) calculateNumBuckets() {
	ht.numBuckets = ht.Size / BUCKET_SIZE
	largestBucketNum := INITIAL_BUCKETS - 1
	for i := 0; i < INITIAL_BUCKETS; i++ {
		lastBucket := ht.lastBucket(i)
		if lastBucket > largestBucketNum && lastBucket < ht.numBuckets {
			largestBucketNum = lastBucket
		}
	}
	ht.numBuckets = largestBucketNum + 1
	usedSize := ht.numBuckets * BUCKET_SIZE
	if usedSize > ht.Size {
		ht.Used = ht.Size
		ht.EnsureSize(usedSize - ht.Used)
	}
	ht.Used = usedSize
	tdlog.Infof("%s: calculated used size is %d", ht.Path, usedSize)
}

// Return number of the next chained bucket.
func (ht *HashTable) nextBucket(bucket int) int {
	if bucket >= ht.numBuckets {
		return 0
	}
	bucketAddr := bucket * BUCKET_SIZE
	nextUint, err := binary.Varint(ht.Buf[bucketAddr : bucketAddr+10])
	next := int(nextUint)
	if next == 0 {
		return 0
	} else if err < 0 || next <= bucket || next >= ht.numBuckets || next < INITIAL_BUCKETS {
		tdlog.CritNoRepeat("Bad hash table - repair ASAP %s", ht.Path)
		return 0
	} else {
		return next
	}
}

// Return number of the last bucket in chain.
func (ht *HashTable) lastBucket(bucket int) int {
	for curr := bucket; ; {
		next := ht.nextBucket(curr)
		if next == 0 {
			return curr
		}
		curr = next
	}
}

// Create and chain a new bucket.
func (ht *HashTable) growBucket(bucket int) {
	ht.EnsureSize(BUCKET_SIZE)
	lastBucketAddr := ht.lastBucket(bucket) * BUCKET_SIZE
	binary.PutVarint(ht.Buf[lastBucketAddr:lastBucketAddr+10], int64(ht.numBuckets))
	ht.Used += BUCKET_SIZE
	ht.numBuckets++
}

// Clear the entire hash table.
func (ht *HashTable) Clear() (err error) {
	if err = ht.DataFile.Clear(); err != nil {
		return
	}
	ht.calculateNumBuckets()
	return
}

// Store the entry into a vacant (invalidated or empty) place in the appropriate bucket.
func (ht *HashTable) Put(key, val int) {
	for bucket, entry := HashKey(key), 0; ; {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER + entry*ENTRY_SIZE
		if ht.Buf[entryAddr] != 1 {
			ht.Buf[entryAddr] = 1
			binary.PutVarint(ht.Buf[entryAddr+1:entryAddr+11], int64(key))
			binary.PutVarint(ht.Buf[entryAddr+11:entryAddr+21], int64(val))
			return
		}
		if entry++; entry == PER_BUCKET {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				ht.growBucket(HashKey(key))
				ht.Put(key, val)
				return
			}
		}
	}
}

// Look up values by key.
func (ht *HashTable) Get(key, limit int) (vals []int) {
	if limit == 0 {
		vals = make([]int, 0, 10)
	} else {
		vals = make([]int, 0, limit)
	}
	for count, entry, bucket := 0, 0, HashKey(key); ; {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER + entry*ENTRY_SIZE
		entryKey, _ := binary.Varint(ht.Buf[entryAddr+1 : entryAddr+11])
		entryVal, _ := binary.Varint(ht.Buf[entryAddr+11 : entryAddr+21])
		if ht.Buf[entryAddr] == 1 {
			if int(entryKey) == key {
				vals = append(vals, int(entryVal))
				if count++; count == limit {
					return
				}
			}
		} else if entryKey == 0 && entryVal == 0 {
			return
		}
		if entry++; entry == PER_BUCKET {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Flag an entry as invalid, so that Get will not return it later on.
func (ht *HashTable) Remove(key, val int) {
	for entry, bucket := 0, HashKey(key); ; {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER + entry*ENTRY_SIZE
		entryKey, _ := binary.Varint(ht.Buf[entryAddr+1 : entryAddr+11])
		entryVal, _ := binary.Varint(ht.Buf[entryAddr+11 : entryAddr+21])
		if ht.Buf[entryAddr] == 1 {
			if int(entryKey) == key && int(entryVal) == val {
				ht.Buf[entryAddr] = 0
				return
			}
		} else if entryKey == 0 && entryVal == 0 {
			return
		}
		if entry++; entry == PER_BUCKET {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Divide the entire hash table into roughly equally sized partitions, and return the start/end key range of the chosen partition.
func GetPartitionRange(partNum, totalParts int) (start int, end int) {
	perPart := INITIAL_BUCKETS / totalParts
	leftOver := INITIAL_BUCKETS % totalParts
	start = partNum * perPart
	if leftOver > 0 {
		if partNum == 0 {
			end += 1
		} else if partNum < leftOver {
			start += partNum
			end += 1
		} else {
			start += leftOver
		}
	}
	end += start + perPart
	if partNum == totalParts-1 {
		end = INITIAL_BUCKETS
	}
	return
}

// Collect entries all the way from "head" bucket to the end of its chained buckets.
func (ht *HashTable) collectEntries(head int) (keys, vals []int) {
	keys = make([]int, 0, PER_BUCKET)
	vals = make([]int, 0, PER_BUCKET)
	var entry, bucket int = 0, head
	for {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER + entry*ENTRY_SIZE
		entryKey, _ := binary.Varint(ht.Buf[entryAddr+1 : entryAddr+11])
		entryVal, _ := binary.Varint(ht.Buf[entryAddr+11 : entryAddr+21])
		if ht.Buf[entryAddr] == 1 {
			keys = append(keys, int(entryKey))
			vals = append(vals, int(entryVal))
		} else if entryKey == 0 && entryVal == 0 {
			return
		}
		if entry++; entry == PER_BUCKET {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Return all entries in the chosen partition.
func (ht *HashTable) GetPartition(partNum, partSize int) (keys, vals []int) {
	rangeStart, rangeEnd := GetPartitionRange(partNum, partSize)
	prealloc := (rangeEnd - rangeStart) * PER_BUCKET
	keys = make([]int, 0, prealloc)
	vals = make([]int, 0, prealloc)
	for head := rangeStart; head < rangeEnd; head++ {
		k, v := ht.collectEntries(head)
		keys = append(keys, k...)
		vals = append(vals, v...)
	}
	return
}
