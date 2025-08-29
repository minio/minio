// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package lambda

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/lambda/event"
	"github.com/minio/minio/internal/config/lambda/target"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/env"
	xnet "github.com/minio/pkg/v3/net"
)

const (
	logSubsys = "notify"
)

func logOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, logSubsys, err, id, errKind...)
}

// ErrTargetsOffline - Indicates single/multiple target failures.
var ErrTargetsOffline = errors.New("one or more targets are offline. Please use `mc admin info --json` to check the offline targets")

// TestSubSysLambdaTargets - tests notification targets of given subsystem
func TestSubSysLambdaTargets(ctx context.Context, cfg config.Config, subSys string, transport *http.Transport) error {
	if err := checkValidLambdaKeysForSubSys(subSys, cfg[subSys]); err != nil {
		return err
	}

	targetList, err := fetchSubSysTargets(ctx, cfg, subSys, transport)
	if err != nil {
		return err
	}

	for _, target := range targetList {
		defer target.Close()
	}

	for _, target := range targetList {
		yes, err := target.IsActive()
		if err == nil && !yes {
			err = ErrTargetsOffline
		}
		if err != nil {
			return fmt.Errorf("error (%s): %w", target.ID(), err)
		}
	}

	return nil
}

func fetchSubSysTargets(ctx context.Context, cfg config.Config, subSys string, transport *http.Transport) (targets []event.Target, err error) {
	if err := checkValidLambdaKeysForSubSys(subSys, cfg[subSys]); err != nil {
		return nil, err
	}

	if subSys == config.LambdaWebhookSubSys {
		webhookTargets, err := GetLambdaWebhook(cfg[config.LambdaWebhookSubSys], transport)
		if err != nil {
			return nil, err
		}
		for id, args := range webhookTargets {
			if !args.Enable {
				continue
			}
			t, err := target.NewWebhookTarget(ctx, id, args, logOnceIf, transport)
			if err != nil {
				return nil, err
			}
			targets = append(targets, t)
		}
	}
	return targets, nil
}

// FetchEnabledTargets - Returns a set of configured TargetList
func FetchEnabledTargets(ctx context.Context, cfg config.Config, transport *http.Transport) (*event.TargetList, error) {
	targetList := event.NewTargetList()
	for _, subSys := range config.LambdaSubSystems.ToSlice() {
		targets, err := fetchSubSysTargets(ctx, cfg, subSys, transport)
		if err != nil {
			return nil, err
		}
		for _, t := range targets {
			if err = targetList.Add(t); err != nil {
				return nil, err
			}
		}
	}
	return targetList, nil
}

// DefaultLambdaKVS - default notification list of kvs.
var (
	DefaultLambdaKVS = map[string]config.KVS{
		config.LambdaWebhookSubSys: DefaultWebhookKVS,
	}
)

// DefaultWebhookKVS - default KV for webhook config
var (
	DefaultWebhookKVS = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   target.WebhookEndpoint,
			Value: "",
		},
		config.KV{
			Key:   target.WebhookAuthToken,
			Value: "",
		},
		config.KV{
			Key:   target.WebhookClientCert,
			Value: "",
		},
		config.KV{
			Key:   target.WebhookClientKey,
			Value: "",
		},
	}
)

func checkValidLambdaKeysForSubSys(subSys string, tgt map[string]config.KVS) error {
	validKVS, ok := DefaultLambdaKVS[subSys]
	if !ok {
		return nil
	}
	for tname, kv := range tgt {
		subSysTarget := subSys
		if tname != config.Default {
			subSysTarget = subSys + config.SubSystemSeparator + tname
		}
		if v, ok := kv.Lookup(config.Enable); ok && v == config.EnableOn {
			if err := config.CheckValidKeys(subSysTarget, kv, validKVS); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetLambdaWebhook - returns a map of registered notification 'webhook' targets
func GetLambdaWebhook(webhookKVS map[string]config.KVS, transport *http.Transport) (
	map[string]target.WebhookArgs, error,
) {
	webhookTargets := make(map[string]target.WebhookArgs)
	for k, kv := range config.Merge(webhookKVS, target.EnvWebhookEnable, DefaultWebhookKVS) {
		enableEnv := target.EnvWebhookEnable
		if k != config.Default {
			enableEnv = enableEnv + config.Default + k
		}
		enabled, err := config.ParseBool(env.Get(enableEnv, kv.Get(config.Enable)))
		if err != nil {
			return nil, err
		}
		if !enabled {
			continue
		}
		urlEnv := target.EnvWebhookEndpoint
		if k != config.Default {
			urlEnv = urlEnv + config.Default + k
		}
		url, err := xnet.ParseHTTPURL(env.Get(urlEnv, kv.Get(target.WebhookEndpoint)))
		if err != nil {
			return nil, err
		}
		authEnv := target.EnvWebhookAuthToken
		if k != config.Default {
			authEnv = authEnv + config.Default + k
		}
		clientCertEnv := target.EnvWebhookClientCert
		if k != config.Default {
			clientCertEnv = clientCertEnv + config.Default + k
		}

		clientKeyEnv := target.EnvWebhookClientKey
		if k != config.Default {
			clientKeyEnv = clientKeyEnv + config.Default + k
		}

		webhookArgs := target.WebhookArgs{
			Enable:     enabled,
			Endpoint:   *url,
			Transport:  transport,
			AuthToken:  env.Get(authEnv, kv.Get(target.WebhookAuthToken)),
			ClientCert: env.Get(clientCertEnv, kv.Get(target.WebhookClientCert)),
			ClientKey:  env.Get(clientKeyEnv, kv.Get(target.WebhookClientKey)),
		}
		if err = webhookArgs.Validate(); err != nil {
			return nil, err
		}
		webhookTargets[k] = webhookArgs
	}
	return webhookTargets, nil
}
