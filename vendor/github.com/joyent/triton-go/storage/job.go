package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/errwrap"
	"github.com/joyent/triton-go/client"
)

type JobClient struct {
	client *client.Client
}

const (
	JobStateDone    = "done"
	JobStateRunning = "running"
)

// JobPhase represents the specification for a map or reduce phase of a Manta
// job.
type JobPhase struct {
	// Type is the type of phase. Must be `map` or `reduce`.
	Type string `json:"type,omitempty"`

	// Assets is an array of objects to be placed in your compute zones.
	Assets []string `json:"assets,omitempty"`

	// Exec is the shell statement to execute. It may be any valid shell
	// command, including pipelines and other shell syntax. You can also
	// execute programs stored in the service by including them in "assets"
	// and referencing them as /assets/$manta_path.
	Exec string `json:"exec"`

	// Init is a shell statement to execute in each compute zone before
	// any tasks are executed. The same constraints apply as to Exec.
	Init string `json:"init"`

	// ReducerCount is an optional number of reducers for this phase. The
	// default value if not specified is 1. The maximum value is 1024.
	ReducerCount uint `json:"count,omitempty"`

	// Memory is the amount of DRAM in MB to be allocated to the compute
	// zone. Valid values are 256, 512, 1024, 2048, 4096 or 8192.
	Memory uint64 `json:"memory,omitempty"`

	// Disk is the amount of disk space in GB to be allocated to the compute
	// zone. Valid values are 2, 4, 8, 16, 32, 64, 128, 256, 512 or 1024.
	Disk uint64 `json:"disk,omitempty"`
}

// JobSummary represents the summary of a compute job in Manta.
type JobSummary struct {
	ModifiedTime time.Time `json:"mtime"`
	ID           string    `json:"name"`
}

// Job represents a compute job in Manta.
type Job struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Phases      []*JobPhase `json:"phases"`
	State       string      `json:"state"`
	Cancelled   bool        `json:"cancelled"`
	InputDone   bool        `json:"inputDone"`
	CreatedTime time.Time   `json:"timeCreated"`
	DoneTime    time.Time   `json:"timeDone"`
	Transient   bool        `json:"transient"`
	Stats       *JobStats   `json:"stats"`
}

// JobStats represents statistics for a compute job in Manta.
type JobStats struct {
	Errors    uint64 `json:"errors"`
	Outputs   uint64 `json:"outputs"`
	Retries   uint64 `json:"retries"`
	Tasks     uint64 `json:"tasks"`
	TasksDone uint64 `json:"tasksDone"`
}

// CreateJobInput represents parameters to a CreateJob operation.
type CreateJobInput struct {
	Name   string      `json:"name"`
	Phases []*JobPhase `json:"phases"`
}

// CreateJobOutput contains the outputs of a CreateJob operation.
type CreateJobOutput struct {
	JobID string
}

// CreateJob submits a new job to be executed. This call is not
// idempotent, so calling it twice will create two jobs.
func (s *JobClient) Create(ctx context.Context, input *CreateJobInput) (*CreateJobOutput, error) {
	path := fmt.Sprintf("/%s/jobs", s.client.AccountName)

	reqInput := client.RequestInput{
		Method: http.MethodPost,
		Path:   path,
		Body:   input,
	}
	respBody, respHeaders, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return nil, errwrap.Wrapf("Error executing CreateJob request: {{err}}", err)
	}

	jobURI := respHeaders.Get("Location")
	parts := strings.Split(jobURI, "/")
	jobID := parts[len(parts)-1]

	response := &CreateJobOutput{
		JobID: jobID,
	}

	return response, nil
}

// AddJobInputs represents parameters to a AddJobInputs operation.
type AddJobInputsInput struct {
	JobID       string
	ObjectPaths []string
}

// AddJobInputs submits inputs to an already created job.
func (s *JobClient) AddInputs(ctx context.Context, input *AddJobInputsInput) error {
	path := fmt.Sprintf("/%s/jobs/%s/live/in", s.client.AccountName, input.JobID)
	headers := &http.Header{}
	headers.Set("Content-Type", "text/plain")

	reader := strings.NewReader(strings.Join(input.ObjectPaths, "\n"))

	reqInput := client.RequestNoEncodeInput{
		Method:  http.MethodPost,
		Path:    path,
		Headers: headers,
		Body:    reader,
	}
	respBody, _, err := s.client.ExecuteRequestNoEncode(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing AddJobInputs request: {{err}}", err)
	}

	return nil
}

// EndJobInputInput represents parameters to a EndJobInput operation.
type EndJobInputInput struct {
	JobID string
}

// EndJobInput submits inputs to an already created job.
func (s *JobClient) EndInput(ctx context.Context, input *EndJobInputInput) error {
	path := fmt.Sprintf("/%s/jobs/%s/live/in/end", s.client.AccountName, input.JobID)

	reqInput := client.RequestNoEncodeInput{
		Method: http.MethodPost,
		Path:   path,
	}
	respBody, _, err := s.client.ExecuteRequestNoEncode(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing EndJobInput request: {{err}}", err)
	}

	return nil
}

// CancelJobInput represents parameters to a CancelJob operation.
type CancelJobInput struct {
	JobID string
}

// CancelJob cancels a job from doing any further work. Cancellation
// is asynchronous and "best effort"; there is no guarantee the job
// will actually stop. For example, short jobs where input is already
// closed will likely still run to completion.
//
// This is however useful when:
// 	- input is still open
// 	- you have a long-running job
func (s *JobClient) Cancel(ctx context.Context, input *CancelJobInput) error {
	path := fmt.Sprintf("/%s/jobs/%s/live/cancel", s.client.AccountName, input.JobID)

	reqInput := client.RequestNoEncodeInput{
		Method: http.MethodPost,
		Path:   path,
	}
	respBody, _, err := s.client.ExecuteRequestNoEncode(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing CancelJob request: {{err}}", err)
	}

	return nil
}

// ListJobsInput represents parameters to a ListJobs operation.
type ListJobsInput struct {
	RunningOnly bool
	Limit       uint64
	Marker      string
}

// ListJobsOutput contains the outputs of a ListJobs operation.
type ListJobsOutput struct {
	Jobs          []*JobSummary
	ResultSetSize uint64
}

// ListJobs returns the list of jobs you currently have.
func (s *JobClient) List(ctx context.Context, input *ListJobsInput) (*ListJobsOutput, error) {
	path := fmt.Sprintf("/%s/jobs", s.client.AccountName)
	query := &url.Values{}
	if input.RunningOnly {
		query.Set("state", "running")
	}
	if input.Limit != 0 {
		query.Set("limit", strconv.FormatUint(input.Limit, 10))
	}
	if input.Marker != "" {
		query.Set("manta_path", input.Marker)
	}

	reqInput := client.RequestInput{
		Method: http.MethodGet,
		Path:   path,
		Query:  query,
	}
	respBody, respHeader, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return nil, errwrap.Wrapf("Error executing ListJobs request: {{err}}", err)
	}

	var results []*JobSummary
	for {
		current := &JobSummary{}
		decoder := json.NewDecoder(respBody)
		if err = decoder.Decode(&current); err != nil {
			if err == io.EOF {
				break
			}
			return nil, errwrap.Wrapf("Error decoding ListJobs response: {{err}}", err)
		}
		results = append(results, current)
	}

	output := &ListJobsOutput{
		Jobs: results,
	}

	resultSetSize, err := strconv.ParseUint(respHeader.Get("Result-Set-Size"), 10, 64)
	if err == nil {
		output.ResultSetSize = resultSetSize
	}

	return output, nil
}

// GetJobInput represents parameters to a GetJob operation.
type GetJobInput struct {
	JobID string
}

// GetJobOutput contains the outputs of a GetJob operation.
type GetJobOutput struct {
	Job *Job
}

// GetJob returns the list of jobs you currently have.
func (s *JobClient) Get(ctx context.Context, input *GetJobInput) (*GetJobOutput, error) {
	path := fmt.Sprintf("/%s/jobs/%s/live/status", s.client.AccountName, input.JobID)

	reqInput := client.RequestInput{
		Method: http.MethodGet,
		Path:   path,
	}
	respBody, _, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return nil, errwrap.Wrapf("Error executing GetJob request: {{err}}", err)
	}

	job := &Job{}
	decoder := json.NewDecoder(respBody)
	if err = decoder.Decode(&job); err != nil {
		return nil, errwrap.Wrapf("Error decoding GetJob response: {{err}}", err)
	}

	return &GetJobOutput{
		Job: job,
	}, nil
}

// GetJobOutputInput represents parameters to a GetJobOutput operation.
type GetJobOutputInput struct {
	JobID string
}

// GetJobOutputOutput contains the outputs for a GetJobOutput operation. It is your
// responsibility to ensure that the io.ReadCloser Items is closed.
type GetJobOutputOutput struct {
	ResultSetSize uint64
	Items         io.ReadCloser
}

// GetJobOutput returns the current "live" set of outputs from a job. Think of
// this like `tail -f`. If error is nil (i.e. the operation is successful), it is
// your responsibility to close the io.ReadCloser named Items in the output.
func (s *JobClient) GetOutput(ctx context.Context, input *GetJobOutputInput) (*GetJobOutputOutput, error) {
	path := fmt.Sprintf("/%s/jobs/%s/live/out", s.client.AccountName, input.JobID)

	reqInput := client.RequestInput{
		Method: http.MethodGet,
		Path:   path,
	}
	respBody, respHeader, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return nil, errwrap.Wrapf("Error executing GetJobOutput request: {{err}}", err)
	}

	output := &GetJobOutputOutput{
		Items: respBody,
	}

	resultSetSize, err := strconv.ParseUint(respHeader.Get("Result-Set-Size"), 10, 64)
	if err == nil {
		output.ResultSetSize = resultSetSize
	}

	return output, nil
}

// GetJobInputInput represents parameters to a GetJobOutput operation.
type GetJobInputInput struct {
	JobID string
}

// GetJobInputOutput contains the outputs for a GetJobOutput operation. It is your
// responsibility to ensure that the io.ReadCloser Items is closed.
type GetJobInputOutput struct {
	ResultSetSize uint64
	Items         io.ReadCloser
}

// GetJobInput returns the current "live" set of inputs from a job. Think of
// this like `tail -f`. If error is nil (i.e. the operation is successful), it is
// your responsibility to close the io.ReadCloser named Items in the output.
func (s *JobClient) GetInput(ctx context.Context, input *GetJobInputInput) (*GetJobInputOutput, error) {
	path := fmt.Sprintf("/%s/jobs/%s/live/in", s.client.AccountName, input.JobID)

	reqInput := client.RequestInput{
		Method: http.MethodGet,
		Path:   path,
	}
	respBody, respHeader, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return nil, errwrap.Wrapf("Error executing GetJobInput request: {{err}}", err)
	}

	output := &GetJobInputOutput{
		Items: respBody,
	}

	resultSetSize, err := strconv.ParseUint(respHeader.Get("Result-Set-Size"), 10, 64)
	if err == nil {
		output.ResultSetSize = resultSetSize
	}

	return output, nil
}

// GetJobFailuresInput represents parameters to a GetJobFailures operation.
type GetJobFailuresInput struct {
	JobID string
}

// GetJobFailuresOutput contains the outputs for a GetJobFailures operation. It is your
// responsibility to ensure that the io.ReadCloser Items is closed.
type GetJobFailuresOutput struct {
	ResultSetSize uint64
	Items         io.ReadCloser
}

// GetJobFailures returns the current "live" set of outputs from a job. Think of
// this like `tail -f`. If error is nil (i.e. the operation is successful), it is
// your responsibility to close the io.ReadCloser named Items in the output.
func (s *JobClient) GetFailures(ctx context.Context, input *GetJobFailuresInput) (*GetJobFailuresOutput, error) {
	path := fmt.Sprintf("/%s/jobs/%s/live/fail", s.client.AccountName, input.JobID)

	reqInput := client.RequestInput{
		Method: http.MethodGet,
		Path:   path,
	}
	respBody, respHeader, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return nil, errwrap.Wrapf("Error executing GetJobFailures request: {{err}}", err)
	}

	output := &GetJobFailuresOutput{
		Items: respBody,
	}

	resultSetSize, err := strconv.ParseUint(respHeader.Get("Result-Set-Size"), 10, 64)
	if err == nil {
		output.ResultSetSize = resultSetSize
	}

	return output, nil
}
