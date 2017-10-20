package renter

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/NebulousLabs/Sia/modules"
)

// Download performs a file download using the passed parameters.
func (r *Renter) Download(p modules.RenterDownloadParameters) error {
	// lookup the file associated with the nickname.
	lockID := r.mu.RLock()
	file, exists := r.files[p.Siapath]
	r.mu.RUnlock(lockID)
	if !exists {
		return errors.New(fmt.Sprintf("no file with that path: %s", p.Siapath))
	}

	isHttpResp := p.Httpwriter != nil

	// validate download parameters
	if p.Async && isHttpResp {
		return errors.New("cannot async download to http response")
	}
	if isHttpResp && p.Destination != "" {
		return errors.New("destination cannot be specified when downloading to http response")
	}
	if !isHttpResp && p.Destination == "" {
		return errors.New("destination not supplied")
	}
	if p.Destination != "" && !filepath.IsAbs(p.Destination) {
		return errors.New("destination must be an absolute path")
	}
	if p.Offset == file.size {
		return errors.New("offset equals filesize")
	}

	// Instantiate the correct DownloadWriter implementation
	// (e.g. content written to file or response body).
	var dw modules.DownloadWriter
	if isHttpResp {
		dw = NewDownloadHttpWriter(p.Httpwriter, p.Offset, p.Length)
	} else {
		dw = NewDownloadFileWriter(p.Destination, p.Offset, p.Length)
	}

	// sentinel: if length == 0, download the entire file
	if p.Length == 0 {
		p.Length = file.size - p.Offset
	}
	// Check whether offset and length is valid.
	if p.Offset < 0 || p.Offset+p.Length > file.size {
		return fmt.Errorf("offset and length combination invalid, max byte is at index %d", file.size-1)
	}

	// Create the download object and add it to the queue.
	d := r.newSectionDownload(file, dw, p.Offset, p.Length)

	lockID = r.mu.Lock()
	r.downloadQueue = append(r.downloadQueue, d)
	r.mu.Unlock(lockID)
	r.newDownloads <- d

	// Block until the download has completed.
	//
	// TODO: Eventually just return the channel to the error instead of the
	// error itself.
	select {
	case <-d.downloadFinished:
		return d.Err()
	case <-r.tg.StopChan():
		return errors.New("download interrupted by shutdown")
	}
}

// DownloadQueue returns the list of downloads in the queue.
func (r *Renter) DownloadQueue() []modules.DownloadInfo {
	lockID := r.mu.RLock()
	defer r.mu.RUnlock(lockID)

	// Order from most recent to least recent.
	downloads := make([]modules.DownloadInfo, len(r.downloadQueue))
	for i := range r.downloadQueue {
		d := r.downloadQueue[len(r.downloadQueue)-i-1]

		downloads[i] = modules.DownloadInfo{
			SiaPath:     d.siapath,
			Destination: d.destination,
			Filesize:    d.length,
			StartTime:   d.startTime,
		}
		downloads[i].Received = atomic.LoadUint64(&d.atomicDataReceived)

		if err := d.Err(); err != nil {
			downloads[i].Error = err.Error()
		}
	}
	return downloads
}
