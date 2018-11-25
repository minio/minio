package api

import "context"

func (c *Sys) Leader() (*LeaderResponse, error) {
	r := c.c.NewRequest("GET", "/v1/sys/leader")

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result LeaderResponse
	err = resp.DecodeJSON(&result)
	return &result, err
}

type LeaderResponse struct {
	HAEnabled                bool   `json:"ha_enabled"`
	IsSelf                   bool   `json:"is_self"`
	LeaderAddress            string `json:"leader_address"`
	LeaderClusterAddress     string `json:"leader_cluster_address"`
	PerfStandby              bool   `json:"performance_standby"`
	PerfStandbyLastRemoteWAL uint64 `json:"performance_standby_last_remote_wal"`
	LastWAL                  uint64 `json:"last_wal"`
}
