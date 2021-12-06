package zcn

type statusCB struct {
	doneCh chan struct{}
	errCh  chan error
}

func (cb *statusCB) Started(allocationId, filePath string, op int, totalBytes int) {

}

func (cb *statusCB) InProgress(allocationId, filePath string, op int, completedBytes int, data []byte) {

}

func (cb *statusCB) Error(allocationID string, filePath string, op int, err error) {
	cb.errCh <- err
}

func (cb *statusCB) Completed(allocationId, filePath string, filename string, mimetype string, size int, op int) {
	cb.doneCh <- struct{}{}
}

func (cb *statusCB) CommitMetaCompleted(request, response string, err error) {

}

func (cb *statusCB) RepairCompleted(filesRepaired int) {

}
