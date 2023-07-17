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

package net

import (
	"fmt"

	"github.com/prometheus/procfs"
)

// GetInterfaceNetStats - get procfs.NetDevLine by interfaceName
func GetInterfaceNetStats(interf string) (procfs.NetDevLine, error) {
	proc, err := procfs.Self()
	if err != nil {
		return procfs.NetDevLine{}, err
	}
	netDev, err := proc.NetDev()
	if err != nil {
		return procfs.NetDevLine{}, err
	}
	ndl, ok := netDev[interf]
	if !ok {
		return procfs.NetDevLine{}, fmt.Errorf("%v interface not found", interf)
	}
	return ndl, nil
}
