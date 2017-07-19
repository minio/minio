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
    <nav className="top-links">
      <Dropdown pullRight id="dropdown-top-links">
        <Dropdown.Toggle noCaret>
          <span href=""><i className="zmdi zmdi-more-vert" /></span>
        </Dropdown.Toggle>
        <Dropdown.Menu className="dropdown-menu-right">
          <li>
            <a onClick={ aboutFunc }>About <i className="zmdi zmdi-info" /></a>
          </li>
          <li>
            <a onClick={ settingsFunc }>Settings <i className="zmdi zmdi-settings" /></a>
          </li>
          <li className="hidden-xs hidden-sm">
            <a onClick={ fullScreenFunc }>Fullscreen <i className="zmdi zmdi-fullscreen" /></a>
          </li>
          <li>
            <a target="_blank" href="https://github.com/minio/minio">Github <i className="zmdi zmdi-github" /></a>
          </li>
          <li>
            <a target="_blank" href="https://docs.minio.io/">Documentation <i className="zmdi zmdi-assignment" /></a>
          </li>
          <li>
            <a target="_blank" href="https://slack.minio.io">Ask for help <i className="zmdi zmdi-help" /></a>
          </li>
          <li>
            <a href="" onClick={ logoutFunc }>Sign Out <i className="zmdi zmdi-sign-in" /></a>
          </li>
        </Dropdown.Menu>
      </Dropdown>
    </nav>
  )
}

export default connect(state => state)(BrowserDropdown)