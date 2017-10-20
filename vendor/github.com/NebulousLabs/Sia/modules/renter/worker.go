package renter

// TODO: Need to make sure that we do not end up with two workers for the same
// host, which could potentially happen over renewals because the contract ids
// will be different.

import (
	"time"

	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/types"
)

type (
	// downloadWork contains instructions to download a piece from a host, and
	// a channel for returning the results.
	downloadWork struct {
		// dataRoot is the MerkleRoot of the data being requested, which serves
		// as an ID when requesting data from the host.
		dataRoot   crypto.Hash
		pieceIndex uint64

		chunkDownload *chunkDownload

		// resultChan is a channel that the worker will use to return the
		// results of the download.
		resultChan chan finishedDownload
	}

	// finishedDownload contains the data and error from performing a download.
	finishedDownload struct {
		chunkDownload *chunkDownload
		data          []byte
		err           error
		pieceIndex    uint64
		workerID      types.FileContractID
	}

	// finishedUpload contains the Merkle root and error from performing an
	// upload.
	finishedUpload struct {
		chunkID    chunkID
		dataRoot   crypto.Hash
		err        error
		pieceIndex uint64
		workerID   types.FileContractID
	}

	// uploadWork contains instructions to upload a piece to a host, and a
	// channel for returning the results.
	uploadWork struct {
		// data is the payload of the upload.
		chunkID    chunkID
		data       []byte
		file       *file
		pieceIndex uint64

		// resultChan is a channel that the worker will use to return the
		// results of the upload.
		resultChan chan finishedUpload
	}

	// A worker listens for work on a certain host.
	worker struct {
		// contractID specifies which contract the worker specifically works
		// with.
		contract   modules.RenterContract
		contractID types.FileContractID

		// If there is work on all three channels, the worker will first do all
		// of the work in the priority download chan, then all of the work in the
		// download chan, and finally all of the work in the upload chan.
		//
		// A busy higher priority channel is able to entirely starve all of the
		// channels with lower priority.
		downloadChan         chan downloadWork // higher priority than all uploads
		killChan             chan struct{}     // highest priority
		priorityDownloadChan chan downloadWork // higher priority than downloads (used for user-initiated downloads)
		uploadChan           chan uploadWork   // lowest priority

		// recentUploadFailure documents the most recent time that an upload
		// has failed.
		consecutiveUploadFailures time.Duration
		recentUploadFailure       time.Time // Only modified by primary repair loop.

		// recentDownloadFailure documents the most recent time that a download
		// has failed.
		recentDownloadFailure time.Time // Only modified by the primary download loop.

		// Utilities.
		renter *Renter
	}
)

// download will perform some download work.
func (w *worker) download(dw downloadWork) {
	d, err := w.renter.hostContractor.Downloader(w.contractID, w.renter.tg.StopChan())
	if err != nil {
		go func() {
			select {
			case dw.resultChan <- finishedDownload{dw.chunkDownload, nil, err, dw.pieceIndex, w.contractID}:
			case <-w.renter.tg.StopChan():
			}
		}()
		return
	}
	defer d.Close()

	data, err := d.Sector(dw.dataRoot)
	go func() {
		select {
		case dw.resultChan <- finishedDownload{dw.chunkDownload, data, err, dw.pieceIndex, w.contractID}:
		case <-w.renter.tg.StopChan():
		}
	}()
}

// upload will perform some upload work.
func (w *worker) upload(uw uploadWork) {
	e, err := w.renter.hostContractor.Editor(w.contractID, w.renter.tg.StopChan())
	if err != nil {
		w.recentUploadFailure = time.Now()
		w.consecutiveUploadFailures++
		go func() {
			select {
			case uw.resultChan <- finishedUpload{uw.chunkID, crypto.Hash{}, err, uw.pieceIndex, w.contractID}:
			case <-w.renter.tg.StopChan():
			}
		}()
		return
	}
	defer e.Close()

	root, err := e.Upload(uw.data)
	if err != nil {
		w.recentUploadFailure = time.Now()
		w.consecutiveUploadFailures++
		go func() {
			select {
			case uw.resultChan <- finishedUpload{uw.chunkID, root, err, uw.pieceIndex, w.contractID}:
			case <-w.renter.tg.StopChan():
			}
		}()
		return
	}

	// Success - reset the consecutive upload failures count.
	w.consecutiveUploadFailures = 0

	// Update the renter metadata.
	addr := e.Address()
	endHeight := e.EndHeight()
	id := w.renter.mu.Lock()
	uw.file.mu.Lock()
	contract, exists := uw.file.contracts[w.contractID]
	if !exists {
		contract = fileContract{
			ID:          w.contractID,
			IP:          addr,
			WindowStart: endHeight,
		}
	}
	contract.Pieces = append(contract.Pieces, pieceData{
		Chunk:      uw.chunkID.index,
		Piece:      uw.pieceIndex,
		MerkleRoot: root,
	})
	uw.file.contracts[w.contractID] = contract
	w.renter.saveFile(uw.file)
	uw.file.mu.Unlock()
	w.renter.mu.Unlock(id)

	go func() {
		select {
		case uw.resultChan <- finishedUpload{uw.chunkID, root, err, uw.pieceIndex, w.contractID}:
		case <-w.renter.tg.StopChan():
		}
	}()
}

// work will perform one unit of work, exiting early if there is a kill signal
// given before work is completed.
func (w *worker) work() {
	// Check for priority downloads.
	select {
	case d := <-w.priorityDownloadChan:
		w.download(d)
		return
	default:
		// do nothing
	}

	// Check for standard downloads.
	select {
	case d := <-w.downloadChan:
		w.download(d)
		return
	default:
		// do nothing
	}

	// None of the priority channels have work, listen on all channels.
	select {
	case d := <-w.downloadChan:
		w.download(d)
		return
	case <-w.killChan:
		return
	case d := <-w.priorityDownloadChan:
		w.download(d)
		return
	case u := <-w.uploadChan:
		w.upload(u)
		return
	case <-w.renter.tg.StopChan():
		return
	}
}

// threadedWorkLoop repeatedly issues work to a worker, stopping when the
// thread group is closed.
func (w *worker) threadedWorkLoop() {
	for {
		// Check if the worker has been killed individually.
		select {
		case <-w.killChan:
			return
		default:
			// do nothing
		}

		if w.renter.tg.Add() != nil {
			return
		}
		w.work()
		w.renter.tg.Done()
	}
}

// updateWorkerPool will grab the set of contracts from the contractor and
// update the worker pool to match.
func (r *Renter) updateWorkerPool(contracts []modules.RenterContract) {
	// Get a map of all the contracts in the contractor.
	newContracts := make(map[types.FileContractID]modules.RenterContract)
	for _, nc := range contracts {
		newContracts[nc.ID] = nc
	}

	// Add a worker for any contract that does not already have a worker.
	for id, contract := range newContracts {
		_, exists := r.workerPool[id]
		if !exists {
			worker := &worker{
				contract:   contract,
				contractID: id,

				downloadChan:         make(chan downloadWork, 1),
				killChan:             make(chan struct{}),
				priorityDownloadChan: make(chan downloadWork, 1),
				uploadChan:           make(chan uploadWork, 1),

				renter: r,
			}
			r.workerPool[id] = worker
			go worker.threadedWorkLoop()
		}
	}

	// Remove a worker for any worker that is not in the set of new contracts.
	for id, worker := range r.workerPool {
		_, exists := newContracts[id]
		if !exists {
			delete(r.workerPool, id)
			close(worker.killChan)
		}
	}
}
