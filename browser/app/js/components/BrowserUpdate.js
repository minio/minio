/*
 * MinIO Cloud Storage (C) 2016 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
                                                   </Tooltip> }> <i className="fa fa-refresh"></i> </OverlayTrigger>
      </a>
    </li>
  )
}

export default connect(state => {
  return {
    latestUiVersion: state.latestUiVersion
  }
})(BrowserUpdate)
