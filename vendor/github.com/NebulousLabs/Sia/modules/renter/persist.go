package renter

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/persist"
	"github.com/NebulousLabs/Sia/types"
)

const (
	PersistFilename = "renter.json"
	ShareExtension  = ".sia"
	logFile         = modules.RenterDir + ".log"
)

var (
	ErrNoNicknames    = errors.New("at least one nickname must be supplied")
	ErrNonShareSuffix = errors.New("suffix of file must be " + ShareExtension)
	ErrBadFile        = errors.New("not a .sia file")
	ErrIncompatible   = errors.New("file is not compatible with current version")

	shareHeader  = [15]byte{'S', 'i', 'a', ' ', 'S', 'h', 'a', 'r', 'e', 'd', ' ', 'F', 'i', 'l', 'e'}
	shareVersion = "0.4"

	saveMetadata = persist.Metadata{
		Header:  "Renter Persistence",
		Version: "0.4",
	}
)

// MarshalSia implements the encoding.SiaMarshaller interface, writing the
// file data to w.
func (f *file) MarshalSia(w io.Writer) error {
	enc := encoding.NewEncoder(w)

	// encode easy fields
	err := enc.EncodeAll(
		f.name,
		f.size,
		f.masterKey,
		f.pieceSize,
		f.mode,
	)
	if err != nil {
		return err
	}
	// COMPATv0.4.3 - encode the bytesUploaded and chunksUploaded fields
	// TODO: the resulting .sia file may confuse old clients.
	err = enc.EncodeAll(f.pieceSize*f.numChunks()*uint64(f.erasureCode.NumPieces()), f.numChunks())
	if err != nil {
		return err
	}

	// encode erasureCode
	switch code := f.erasureCode.(type) {
	case *rsCode:
		err = enc.EncodeAll(
			"Reed-Solomon",
			uint64(code.dataPieces),
			uint64(code.numPieces-code.dataPieces),
		)
		if err != nil {
			return err
		}
	default:
		if build.DEBUG {
			panic("unknown erasure code")
		}
		return errors.New("unknown erasure code")
	}
	// encode contracts
	if err := enc.Encode(uint64(len(f.contracts))); err != nil {
		return err
	}
	for _, c := range f.contracts {
		if err := enc.Encode(c); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalSia implements the encoding.SiaUnmarshaller interface,
// reconstructing a file from the encoded bytes read from r.
func (f *file) UnmarshalSia(r io.Reader) error {
	dec := encoding.NewDecoder(r)

	// COMPATv0.4.3 - decode bytesUploaded and chunksUploaded into dummy vars.
	var bytesUploaded, chunksUploaded uint64

	// Decode easy fields.
	err := dec.DecodeAll(
		&f.name,
		&f.size,
		&f.masterKey,
		&f.pieceSize,
		&f.mode,
		&bytesUploaded,
		&chunksUploaded,
	)
	if err != nil {
		return err
	}

	// Decode erasure coder.
	var codeType string
	if err := dec.Decode(&codeType); err != nil {
		return err
	}
	switch codeType {
	case "Reed-Solomon":
		var nData, nParity uint64
		err = dec.DecodeAll(
			&nData,
			&nParity,
		)
		if err != nil {
			return err
		}
		rsc, err := NewRSCode(int(nData), int(nParity))
		if err != nil {
			return err
		}
		f.erasureCode = rsc
	default:
		return errors.New("unrecognized erasure code type: " + codeType)
	}

	// Decode contracts.
	var nContracts uint64
	if err := dec.Decode(&nContracts); err != nil {
		return err
	}
	f.contracts = make(map[types.FileContractID]fileContract)
	var contract fileContract
	for i := uint64(0); i < nContracts; i++ {
		if err := dec.Decode(&contract); err != nil {
			return err
		}
		f.contracts[contract.ID] = contract
	}
	return nil
}

// saveFile saves a file to the renter directory.
func (r *Renter) saveFile(f *file) error {
	// Create directory structure specified in nickname.
	fullPath := filepath.Join(r.persistDir, f.name+ShareExtension)
	err := os.MkdirAll(filepath.Dir(fullPath), 0700)
	if err != nil {
		return err
	}

	// Open SafeFile handle.
	handle, err := persist.NewSafeFile(filepath.Join(r.persistDir, f.name+ShareExtension))
	if err != nil {
		return err
	}
	defer handle.Close()

	// Write file data.
	err = shareFiles([]*file{f}, handle)
	if err != nil {
		return err
	}

	// Commit the SafeFile.
	return handle.CommitSync()
}

// saveSync stores the current renter data to disk and then syncs to disk.
func (r *Renter) saveSync() error {
	data := struct {
		Tracking map[string]trackedFile
	}{r.tracking}

	return persist.SaveJSON(saveMetadata, data, filepath.Join(r.persistDir, PersistFilename))
}

// load fetches the saved renter data from disk.
func (r *Renter) load() error {
	// Recursively load all files found in renter directory. Errors
	// encountered during loading are logged, but are not considered fatal.
	err := filepath.Walk(r.persistDir, func(path string, info os.FileInfo, err error) error {
		// This error is non-nil if filepath.Walk couldn't stat a file or
		// folder.
		if err != nil {
			r.log.Println("WARN: could not stat file or folder during walk:", err)
			return nil
		}

		// Skip folders and non-sia files.
		if info.IsDir() || filepath.Ext(path) != ShareExtension {
			return nil
		}

		// Open the file.
		file, err := os.Open(path)
		if err != nil {
			r.log.Println("ERROR: could not open .sia file:", err)
			return nil
		}
		defer file.Close()

		// Load the file contents into the renter.
		_, err = r.loadSharedFiles(file)
		if err != nil {
			r.log.Println("ERROR: could not load .sia file:", err)
			return nil
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Load contracts, repair set, and entropy.
	data := struct {
		Tracking  map[string]trackedFile
		Repairing map[string]string // COMPATv0.4.8
	}{}
	err = persist.LoadJSON(saveMetadata, &data, filepath.Join(r.persistDir, PersistFilename))
	if err != nil {
		return err
	}
	if data.Tracking != nil {
		r.tracking = data.Tracking
	}

	return nil
}

// shareFiles writes the specified files to w. First a header is written,
// followed by the gzipped concatenation of each file.
func shareFiles(files []*file, w io.Writer) error {
	// Write header.
	err := encoding.NewEncoder(w).EncodeAll(
		shareHeader,
		shareVersion,
		uint64(len(files)),
	)
	if err != nil {
		return err
	}

	// Create compressor.
	zip, _ := gzip.NewWriterLevel(w, gzip.BestSpeed)
	enc := encoding.NewEncoder(zip)

	// Encode each file.
	for _, f := range files {
		err = enc.Encode(f)
		if err != nil {
			return err
		}
	}

	return zip.Close()
}

// ShareFile saves the specified files to shareDest.
func (r *Renter) ShareFiles(nicknames []string, shareDest string) error {
	lockID := r.mu.RLock()
	defer r.mu.RUnlock(lockID)

	// TODO: consider just appending the proper extension.
	if filepath.Ext(shareDest) != ShareExtension {
		return ErrNonShareSuffix
	}

	handle, err := os.Create(shareDest)
	if err != nil {
		return err
	}
	defer handle.Close()

	// Load files from renter.
	files := make([]*file, len(nicknames))
	for i, name := range nicknames {
		f, exists := r.files[name]
		if !exists {
			return ErrUnknownPath
		}
		files[i] = f
	}

	err = shareFiles(files, handle)
	if err != nil {
		os.Remove(shareDest)
		return err
	}

	return nil
}

// ShareFilesAscii returns the specified files in ASCII format.
func (r *Renter) ShareFilesAscii(nicknames []string) (string, error) {
	lockID := r.mu.RLock()
	defer r.mu.RUnlock(lockID)

	// Load files from renter.
	files := make([]*file, len(nicknames))
	for i, name := range nicknames {
		f, exists := r.files[name]
		if !exists {
			return "", ErrUnknownPath
		}
		files[i] = f
	}

	buf := new(bytes.Buffer)
	err := shareFiles(files, base64.NewEncoder(base64.URLEncoding, buf))
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

// loadSharedFiles reads .sia data from reader and registers the contained
// files in the renter. It returns the nicknames of the loaded files.
func (r *Renter) loadSharedFiles(reader io.Reader) ([]string, error) {
	// read header
	var header [15]byte
	var version string
	var numFiles uint64
	err := encoding.NewDecoder(reader).DecodeAll(
		&header,
		&version,
		&numFiles,
	)
	if err != nil {
		return nil, err
	} else if header != shareHeader {
		return nil, ErrBadFile
	} else if version != shareVersion {
		return nil, ErrIncompatible
	}

	// Create decompressor.
	unzip, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	dec := encoding.NewDecoder(unzip)

	// Read each file.
	files := make([]*file, numFiles)
	for i := range files {
		files[i] = new(file)
		err := dec.Decode(files[i])
		if err != nil {
			return nil, err
		}

		// Make sure the file's name does not conflict with existing files.
		dupCount := 0
		origName := files[i].name
		for {
			_, exists := r.files[files[i].name]
			if !exists {
				break
			}
			dupCount++
			files[i].name = origName + "_" + strconv.Itoa(dupCount)
		}
	}

	// Add files to renter.
	names := make([]string, numFiles)
	for i, f := range files {
		r.files[f.name] = f
		names[i] = f.name
	}
	// Save the files.
	for _, f := range files {
		r.saveFile(f)
	}

	return names, nil
}

// initPersist handles all of the persistence initialization, such as creating
// the persistence directory and starting the logger.
func (r *Renter) initPersist() error {
	// Create the perist directory if it does not yet exist.
	err := os.MkdirAll(r.persistDir, 0700)
	if err != nil {
		return err
	}

	// Initialize the logger.
	r.log, err = persist.NewFileLogger(filepath.Join(r.persistDir, logFile))
	if err != nil {
		return err
	}

	// Load the prior persistence structures.
	err = r.load()
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// LoadSharedFiles loads a .sia file into the renter. It returns the nicknames
// of the loaded files.
func (r *Renter) LoadSharedFiles(filename string) ([]string, error) {
	lockID := r.mu.Lock()
	defer r.mu.Unlock(lockID)

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return r.loadSharedFiles(file)
}

// LoadSharedFilesAscii loads an ASCII-encoded .sia file into the renter. It
// returns the nicknames of the loaded files.
func (r *Renter) LoadSharedFilesAscii(asciiSia string) ([]string, error) {
	lockID := r.mu.Lock()
	defer r.mu.Unlock(lockID)

	dec := base64.NewDecoder(base64.URLEncoding, bytes.NewBufferString(asciiSia))
	return r.loadSharedFiles(dec)
}
