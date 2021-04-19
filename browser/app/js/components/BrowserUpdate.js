/*
 * Copyright (c) 2015-2021 MinIO, Inc.
 *
 * This file is part of MinIO Object Storage stack
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import React from 'react'
import connect from 'react-redux/lib/components/connect'

import Tooltip from 'react-bootstrap/lib/Tooltip'
import OverlayTrigger from 'react-bootstrap/lib/OverlayTrigger'

let BrowserUpdate = ({latestUiVersion}) => {
  // Don't show an update if we're already updated!
  if (latestUiVersion === currentUiVersion) return ( <noscript></noscript> )

  return (
    <li className="hidden-xs hidden-sm">
      <a href="">
        <OverlayTrigger placement="left" overlay={ <Tooltip id="tt-version-update">
                                                     New update available. Click to refresh.
                                                   </Tooltip> }> <i className="fas fa-sync"></i> </OverlayTrigger>
      </a>
    </li>
  )
}

export default connect(state => {
  return {
    latestUiVersion: state.latestUiVersion
  }
})(BrowserUpdate)
