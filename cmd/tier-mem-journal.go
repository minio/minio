// Copyright (c) 2015-2021 MinIO, Inc.
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

package cmd

import (
	"context"
	"fmt"

	"github.com/minio/minio/internal/logger"
)

type tierMemJournal struct {
	entries chan jentry
}

func newTierMemJoural(nevents int) *tierMemJournal {
	return &tierMemJournal{
		entries: make(chan jentry, nevents),
	}
}

func (j *tierMemJournal) processEntries(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case entry := <-j.entries:
			logger.LogIf(ctx, deleteObjectFromRemoteTier(ctx, entry.ObjName, entry.VersionID, entry.TierName))
		}
	}
}

func (j *tierMemJournal) AddEntry(je jentry) error {
	select {
	case j.entries <- je:
	default:
		return fmt.Errorf("failed to remove tiered content at %s with version %s from tier %s, will be retried later.",
			je.ObjName, je.VersionID, je.TierName)
	}
	return nil
}
