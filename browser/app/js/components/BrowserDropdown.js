/*
 * Minio Cloud Storage (C) 2016, 2017 Minio, Inc.
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
import Dropdown from 'react-bootstrap/lib/Dropdown'

let BrowserDropdown = ({fullScreenFunc, aboutFunc, settingsFunc, logoutFunc}) => {
  return (
    <li>
      <Dropdown pullRight id="top-right-menu">
        <Dropdown.Toggle noCaret>
          <i className="fa fa-reorder"></i>
        </Dropdown.Toggle>
        <Dropdown.Menu className="dropdown-menu-right">
          <li>
            <a target="_blank" href="https://github.com/minio/minio">Github <i className="fa fa-github"></i></a>
          </li>
          <li>
            <a href="" onClick={ fullScreenFunc }>Fullscreen <i className="fa fa-expand"></i></a>
          </li>
          <li>
            <a target="_blank" href="https://docs.minio.io/">Documentation <i className="fa fa-book"></i></a>
          </li>
          <li>
            <a target="_blank" href="https://slack.minio.io">Ask for help <i className="fa fa-question-circle"></i></a>
          </li>
          <li>
            <a href="" onClick={ aboutFunc }>About <i className="fa fa-info-circle"></i></a>
          </li>
          <li>
            <a href="" onClick={ settingsFunc }>Change Password <i className="fa fa-cog"></i></a>
          </li>
          <li>
            <a href="" onClick={ logoutFunc }>Sign Out <i className="fa fa-sign-out"></i></a>
          </li>
        </Dropdown.Menu>
      </Dropdown>
    </li>
  )
}

export default connect(state => state)(BrowserDropdown)
