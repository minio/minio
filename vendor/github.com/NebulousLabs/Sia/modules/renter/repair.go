package renter

// TODO: There are no download-to-reupload strategies implemented.

// TODO: The chunkStatus stuff needs to recognize when two different contract
// ids are actually a part of the same file contract.

import (
	"errors"
	"io"
	"os"
	"time"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/types"
)

var (
	// errFileDeleted indicates that a chunk which is trying to be repaired
	// cannot be found in the renter.
	errFileDeleted = errors.New("cannot repair chunk as the file is not being tracked by the renter")
)

type (
	// chunkStatus contains information about a chunk to assist with repairing
	// the chunk.
	chunkStatus struct {
		// contracts is the set of file contracts which are already storing
		// pieces for the chunk.
		//
		// pieces contains the indices of the pieces that have already been
		// uploaded for this chunk.
		//
		// recordedGaps indicates the value that this chunk has recorded in the
		// gapCounts map.
		activePieces int
		contracts    map[types.FileContractID]struct{}
		pieces       map[uint64]struct{}
		recordedGaps int
		totalPieces  int
	}

	// chunkID can be used to uniquely identify a chunk within the repair
	// matrix.
	chunkID struct {
		index     uint64 // the index of the chunk within its file.
		masterkey crypto.TwofishKey
	}

	// repairState tracks a bunch of chunks that are being actively repaired.
	repairState struct {
		// activeWorkers is the set of workers that is currently performing
		// work, thus are currently unavailable but will become available soon.
		//
		// availableWorkers is a set of workers that are currently available to
		// perform work, and do not have any current jobs.
		//
		// gapCounts tracks how many chunks have each number of gaps. This is
		// used to drive uploading optimizations.
		//
		// incompleteChunks tracks a set of chunks that don't yet have full
		// redundancy, along with information about which pieces and contracts
		// aren't being used.
		//
		// downloadingChunks tracks the set of chunks that are currently being
		// downloaded in order to be re-uploaded.
		//
		// cachedChunks tracks the set of chunks that have recently been retreived
		// from hosts.
		//
		// workerSet tracks the set of workers which can be used for uploading.
		activeWorkers     map[types.FileContractID]*worker
		availableWorkers  map[types.FileContractID]*worker
		gapCounts         map[int]int
		incompleteChunks  map[chunkID]*chunkStatus
		downloadingChunks map[chunkID]struct{}
		cachedChunks      map[chunkID][]byte
		resultChan        chan finishedUpload
	}
)

// numGaps returns the number of gaps that a chunk has.
func (cs *chunkStatus) numGaps(rs *repairState) int {
	incompatContracts := 0
	for contract := range cs.contracts {
		_, exists1 := rs.activeWorkers[contract]
		_, exists2 := rs.availableWorkers[contract]
		if exists1 || exists2 {
			incompatContracts++
		}
	}
	contractGaps := len(rs.activeWorkers) + len(rs.availableWorkers) - incompatContracts
	pieceGaps := cs.totalPieces - len(cs.pieces)

	if contractGaps < pieceGaps {
		return contractGaps
	}
	return pieceGaps
}

// managedAddFileToRepairState will take a file and add each of the incomplete
// chunks to the repair state, along with data about which pieces need
// attention.
func (r *Renter) managedAddFileToRepairState(rs *repairState, file *file) {
	// Check that the file is being tracked, and therefore candidate for
	// repair.
	id := r.mu.RLock()
	file.mu.RLock()
	_, exists := r.tracking[file.name]
	file.mu.RUnlock()
	r.mu.RUnlock(id)
	if !exists {
		return
	}

	// Fetch the list of potential contracts from the repair state.
	contracts := make([]types.FileContractID, 0)
	for contract := range rs.activeWorkers {
		contracts = append(contracts, contract)
	}
	for contract := range rs.availableWorkers {
		contracts = append(contracts, contract)
	}

	// Create the data structures that allow us to fill out the status for each
	// chunk.
	chunkCount := file.numChunks()
	availablePieces := make([]map[uint64]struct{}, chunkCount)
	utilizedContracts := make([]map[types.FileContractID]struct{}, chunkCount)
	for i := uint64(0); i < chunkCount; i++ {
		availablePieces[i] = make(map[uint64]struct{})
		utilizedContracts[i] = make(map[types.FileContractID]struct{})
	}

	// Iterate through each contract and figure out which pieces are available.
	file.mu.RLock()
	var fileContracts []fileContract
	for _, c := range file.contracts {
		fileContracts = append(fileContracts, c)
	}
	file.mu.RUnlock()
	for _, contract := range fileContracts {
		// Check whether this contract is offline. Even if the contract is
		// offline, we want to record that the chunk has attempted to use this
		// contract.
		id := r.hostContractor.ResolveID(contract.ID)
		stable := !r.hostContractor.IsOffline(id) && r.hostContractor.GoodForRenew(id)

		// Scan all of the pieces of the contract.
		for _, piece := range contract.Pieces {
			utilizedContracts[piece.Chunk][id] = struct{}{}

			// Only mark the piece as complete if the piece can be recovered.
			if stable {
				availablePieces[piece.Chunk][piece.Piece] = struct{}{}
			}
		}
	}

	// Create the chunkStatus object for each chunk and add it to the set of
	// incomplete chunks.
	for i := uint64(0); i < chunkCount; i++ {
		// Skip this chunk if all pieces have been uploaded.
		if len(availablePieces[i]) >= file.erasureCode.NumPieces() {
			continue
		}

		// Skip this chunk if it's already in the set of incomplete chunks.
		cid := chunkID{i, file.masterKey}
		_, exists := rs.incompleteChunks[cid]
		if exists {
			continue
		}

		// Create the chunkStatus object and add it to the set of incomplete
		// chunks.
		cs := &chunkStatus{
			contracts:   utilizedContracts[i],
			pieces:      availablePieces[i],
			totalPieces: file.erasureCode.NumPieces(),
		}
		cs.recordedGaps = cs.numGaps(rs)
		rs.incompleteChunks[cid] = cs
		rs.gapCounts[cs.recordedGaps]++
	}
}

// managedRepairIteration does a full file repair iteration, which includes
// scanning all of the files for missing pieces and attempting repair them by
// uploading to chunks.
func (r *Renter) managedRepairIteration(rs *repairState) {
	// Wait for work if there is nothing to do.
	if len(rs.activeWorkers) == 0 && len(rs.incompleteChunks) == 0 {
		select {
		case <-r.tg.StopChan():
			return
		case file := <-r.newRepairs:
			r.managedAddFileToRepairState(rs, file)
			return
		}
	}

	// Reset the available workers.
	contracts := r.hostContractor.Contracts()
	id := r.mu.Lock()
	r.updateWorkerPool(contracts)
	rs.availableWorkers = make(map[types.FileContractID]*worker)
	for id, worker := range r.workerPool {
		// Ignore the workers that are not good for uploading.
		if !worker.contract.GoodForUpload {
			continue
		}

		// Ignore workers that are already in the active set of workers.
		_, exists := rs.activeWorkers[worker.contractID]
		if exists {
			continue
		}

		// Ignore workers that have had an upload failure recently. The cooldown
		// time scales exponentially as the number of consecutive failures grow,
		// stopping at 10 doublings, or about 17 hours total cooldown.
		penalty := uint64(worker.consecutiveUploadFailures)
		if worker.consecutiveUploadFailures > time.Duration(maxConsecutivePenalty) {
			penalty = uint64(maxConsecutivePenalty)
		}
		if time.Since(worker.recentUploadFailure) < uploadFailureCooldown*(1<<penalty) {
			continue
		}

		rs.availableWorkers[id] = worker
	}
	r.mu.Unlock(id)

	// Determine the maximum number of gaps of any chunk in the repair matrix.
	maxGaps := 0
	for i, gaps := range rs.gapCounts {
		if i > maxGaps && gaps > 0 {
			maxGaps = i
		}
	}

	// prune the chunk cache
	for cid := range rs.cachedChunks {
		if len(rs.cachedChunks) <= maxChunkCacheSize {
			break
		}
		delete(rs.cachedChunks, cid)
	}

	// Scan through the chunks until a candidate for uploads is found.
	var chunksToDelete []chunkID
	for chunkID, chunkStatus := range rs.incompleteChunks {
		// check if the chunk is currently being downloaded for recovery
		if _, downloading := rs.downloadingChunks[chunkID]; downloading {
			continue
		}
		// Update the number of gaps for this chunk.
		numGaps := chunkStatus.numGaps(rs)
		rs.gapCounts[chunkStatus.recordedGaps]--
		rs.gapCounts[numGaps]++
		chunkStatus.recordedGaps = numGaps

		// Remove this chunk from the set of incomplete chunks if it has been
		// completed and there are no workers still working on it.
		if numGaps == 0 && chunkStatus.activePieces == 0 {
			chunksToDelete = append(chunksToDelete, chunkID)
			continue
		}

		// Skip this chunk if it does not have enough gaps.
		if maxGaps >= minPiecesRepair && numGaps < minPiecesRepair {
			continue
		}

		// Determine the set of useful workers - workers that are both
		// available and able to repair this chunk.
		var usefulWorkers []types.FileContractID
		for workerID, worker := range rs.availableWorkers {
			_, exists := chunkStatus.contracts[workerID]
			if !exists && worker.contract.GoodForUpload {
				usefulWorkers = append(usefulWorkers, workerID)
			}
		}

		// Skip this chunk if the set of useful workers does not meet the
		// minimum pieces requirement.
		if maxGaps >= minPiecesRepair && len(usefulWorkers) < minPiecesRepair {
			continue
		}

		// Skip this chunk if the set of useful workers is not complete, and
		// the maxGaps value is less than the minPiecesRepair value.
		if maxGaps < minPiecesRepair && len(usefulWorkers) < numGaps {
			continue
		}

		// Send off the work.
		err := r.managedScheduleChunkRepair(rs, chunkID, chunkStatus, usefulWorkers)
		if err != nil {
			r.log.Println("Unable to repair chunk:", err)
			chunksToDelete = append(chunksToDelete, chunkID)
			continue
		}
	}
	for _, cid := range chunksToDelete {
		delete(rs.incompleteChunks, cid)
	}

	// Block until some of the workers return.
	r.managedWaitOnRepairWork(rs)
}

// managedDownloadChunkData downloads the requested chunk from Sia, for use in
// the repair loop.
func (r *Renter) managedDownloadChunkData(rs *repairState, file *file, offset uint64, chunkIndex uint64, chunkID chunkID) ([]byte, error) {
	rs.downloadingChunks[chunkID] = struct{}{}
	defer delete(rs.downloadingChunks, chunkID)

	downloadSize := file.chunkSize()
	if offset+downloadSize > file.size {
		downloadSize = file.size - offset
	}

	// create a DownloadBufferWriter for the chunk
	buf := NewDownloadBufferWriter(file.chunkSize(), int64(offset))

	// create the download object and push it on to the download queue
	d := r.newSectionDownload(file, buf, offset, downloadSize)
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case r.newDownloads <- d:
		case <-done:
		}
	}()

	// wait for the download to complete and return the data
	select {
	case <-d.downloadFinished:
		return buf.Bytes(), d.Err()
	case <-r.tg.StopChan():
		return nil, errors.New("chunk download interrupted by shutdown")
	case <-time.After(chunkDownloadTimeout):
		return nil, errors.New("chunk download timed out")
	}
}

// managedGetChunkData grabs the requested `chunkID` from the file, in order to
// repair the file. If the `trackedFile` can be found on disk, grab the chunk
// from the file, otherwise attempt to queue a new download for only that chunk
// and return the downloaded chunk.
func (r *Renter) managedGetChunkData(rs *repairState, file *file, trackedFile trackedFile, chunkID chunkID) ([]byte, error) {
	chunkIndex := chunkID.index
	offset := chunkIndex * file.chunkSize()

	// try to read the chunk from disk
	f, err := os.Open(trackedFile.RepairPath)
	if err != nil {
		// if that fails, try to download the chunk
		return r.managedDownloadChunkData(rs, file, offset, chunkIndex, chunkID)
	}
	defer f.Close()

	chunkData := make([]byte, file.chunkSize())
	_, err = f.ReadAt(chunkData, int64(offset))
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		// TODO: We should be doing better error handling here - shouldn't be
		// running into ErrUnexpectedEOF intentionally because if it happens
		// unintentionally we will believe that the chunk was read from memory
		// correctly.

		return nil, err
	}

	return chunkData, nil
}

// managedScheduleChunkRepair takes a chunk and schedules some repair on that
// chunk using the chunk state and a list of workers.
func (r *Renter) managedScheduleChunkRepair(rs *repairState, chunkID chunkID, chunkStatus *chunkStatus, usefulWorkers []types.FileContractID) error {
	// Check that the file is still in the renter.
	id := r.mu.RLock()
	var file *file
	for _, f := range r.files {
		if f.masterKey == chunkID.masterkey {
			file = f
			break
		}
	}
	var meta trackedFile
	var exists bool
	if file != nil {
		meta, exists = r.tracking[file.name]
	}
	r.mu.RUnlock(id)
	if !exists {
		return errFileDeleted
	}

	// read the chunk into memory
	// check the cache first
	var chunkData []byte
	if cachedData, exists := rs.cachedChunks[chunkID]; exists {
		chunkData = cachedData
	} else {
		data, err := r.managedGetChunkData(rs, file, meta, chunkID)
		if err != nil {
			return build.ExtendErr("unable to get repair chunk:", err)
		}
		chunkData = data
		rs.cachedChunks[chunkID] = data
	}

	// Erasure code the pieces.
	pieces, err := file.erasureCode.Encode(chunkData)
	if err != nil {
		return build.ExtendErr("unable to erasure code chunk data", err)
	}

	// Get the set of pieces that are missing from the chunk.
	var missingPieces []uint64
	for i := uint64(0); i < uint64(file.erasureCode.NumPieces()); i++ {
		_, exists := chunkStatus.pieces[i]
		if !exists {
			missingPieces = append(missingPieces, i)
		}
	}

	// Truncate the pieces so that they match the size of the useful workers.
	if len(usefulWorkers) < len(missingPieces) {
		missingPieces = missingPieces[:len(usefulWorkers)]
	}

	// Encrypt the missing pieces.
	for _, missingPiece := range missingPieces {
		key := deriveKey(file.masterKey, chunkID.index, uint64(missingPiece))
		pieces[missingPiece] = key.EncryptBytes(pieces[missingPiece])
	}

	// Give each piece to a worker in the set of useful workers.
	for len(usefulWorkers) > 0 && len(missingPieces) > 0 {
		uw := uploadWork{
			chunkID:    chunkID,
			data:       pieces[missingPieces[0]],
			file:       file,
			pieceIndex: missingPieces[0],

			resultChan: rs.resultChan,
		}
		// Grab the worker, and update the worker tracking in the repair state.
		worker := rs.availableWorkers[usefulWorkers[0]]
		rs.activeWorkers[usefulWorkers[0]] = worker
		delete(rs.availableWorkers, usefulWorkers[0])

		chunkStatus.activePieces++
		chunkStatus.contracts[usefulWorkers[0]] = struct{}{}
		chunkStatus.pieces[missingPieces[0]] = struct{}{}

		// Update the number of gaps for this chunk.
		numGaps := chunkStatus.numGaps(rs)
		rs.gapCounts[chunkStatus.recordedGaps]--
		rs.gapCounts[numGaps]++
		chunkStatus.recordedGaps = numGaps

		// Update the set of useful workers and the set of missing pieces.
		missingPieces = missingPieces[1:]
		usefulWorkers = usefulWorkers[1:]

		// Deliver the payload to the worker.
		select {
		case worker.uploadChan <- uw:
		default:
			r.log.Critical("Worker is supposed to be available, but upload work channel is full")
			worker.uploadChan <- uw
		}
	}
	return nil
}

// managedWaitOnRepairWork will block until a worker returns from an upload,
// handling the results.
func (r *Renter) managedWaitOnRepairWork(rs *repairState) {
	// If there are no active workers, return early.
	if len(rs.activeWorkers) == 0 {
		return
	}

	// Wait for an upload to finish.
	var finishedUpload finishedUpload
	select {
	case finishedUpload = <-rs.resultChan:
	case file := <-r.newRepairs:
		r.managedAddFileToRepairState(rs, file)
		return
	case <-r.tg.StopChan():
		return
	}

	// Mark that the worker of this chunk has completed its work.
	if cs, ok := rs.incompleteChunks[finishedUpload.chunkID]; !ok {
		// The file was deleted mid-upload. Add the worker back to the set of
		// available workers.
		rs.availableWorkers[finishedUpload.workerID] = rs.activeWorkers[finishedUpload.workerID]
		delete(rs.activeWorkers, finishedUpload.workerID)
		return
	} else {
		cs.activePieces--
	}

	// If there was no error, add the worker back to the set of
	// available workers and wait for the next worker.
	if finishedUpload.err == nil {
		rs.availableWorkers[finishedUpload.workerID] = rs.activeWorkers[finishedUpload.workerID]
		delete(rs.activeWorkers, finishedUpload.workerID)
		return
	}

	// Log the error and retire the worker.
	r.log.Debugln("Error while performing upload to", finishedUpload.workerID, "::", finishedUpload.err)
	delete(rs.activeWorkers, finishedUpload.workerID)

	// Indicate in the set of incomplete chunks that this piece was not
	// completed.
	rs.incompleteChunks[finishedUpload.chunkID].pieces[finishedUpload.pieceIndex] = struct{}{}
}

// threadedQueueRepairs is a goroutine that runs in the background and
// continuously adds files to the repair loop, slow enough that it's not a
// resource burden but fast enough that no file is ever at risk.
//
// NOTE: This loop is pretty naive in terms of work management. As the number
// of files goes up, and as the number of chunks per file goes up, this will
// become a performance bottleneck, and even inhibit repair progress.
func (r *Renter) threadedQueueRepairs() {
	for {
		// Compress the set of files into a slice.
		id := r.mu.RLock()
		var files []*file
		for _, file := range r.files {
			if _, ok := r.tracking[file.name]; ok {
				// Only repair files that are being tracked.
				files = append(files, file)
			}
		}
		r.mu.RUnlock(id)

		// Add files.
		for _, file := range files {
			// Send the file down the repair channel.
			select {
			case r.newRepairs <- file:
			case <-r.tg.StopChan():
				return
			}
		}

		// Chill out for an extra 15 minutes before going through the files
		// again.
		select {
		case <-time.After(repairQueueInterval):
		case <-r.tg.StopChan():
			return
		}
	}
}

// threadedRepairLoop improves the health of files tracked by the renter by
// reuploading their missing pieces. Multiple repair attempts may be necessary
// before the file reaches full redundancy.
func (r *Renter) threadedRepairLoop() {
	rs := &repairState{
		activeWorkers:     make(map[types.FileContractID]*worker),
		availableWorkers:  make(map[types.FileContractID]*worker),
		gapCounts:         make(map[int]int),
		incompleteChunks:  make(map[chunkID]*chunkStatus),
		cachedChunks:      make(map[chunkID][]byte),
		downloadingChunks: make(map[chunkID]struct{}),
		resultChan:        make(chan finishedUpload),
	}
	for {
		if r.tg.Add() != nil {
			return
		}
		r.managedRepairIteration(rs)
		r.tg.Done()
	}
}
