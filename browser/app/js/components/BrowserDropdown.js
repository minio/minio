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

import iconMore from '../../img/icons/more-h.svg';
import iconGithub from '../../img/icons/github.svg';
import iconDocs from '../../img/icons/docs.svg';
import iconHelp from '../../img/icons/help.svg';
import iconOff from '../../img/icons/off.svg';
import iconInfo from '../../img/icons/info.svg';
import iconSettings from '../../img/icons/settings.svg';
import iconFullscreen from '../../img/icons/fullscreen.svg';


let BrowserDropdown = ({fullScreenFunc, aboutFunc, settingsFunc, logoutFunc}) => {
  return (
    <nav className="top-links">
      <div onClick={ aboutFunc }><img src={ iconInfo } alt=""/></div>
      <div onClick={ settingsFunc } href=""><img src={ iconSettings } alt=""/></div>
      <div onClick={ fullScreenFunc }><img src={ iconFullscreen } alt=""/></div>

      <Dropdown pullRight id="dropdown-top-links">
        <Dropdown.Toggle noCaret>
          <span href=""><img src={ iconMore } alt=""/></span>
        </Dropdown.Toggle>
        <Dropdown.Menu className="dropdown-menu-right">
          <li>
            <a target="_blank" href="https://github.com/minio/minio">Github <img src={ iconGithub } alt=""/></a>
          </li>
          <li>
            <a target="_blank" href="https://docs.minio.io/">Documentation  <img src={ iconDocs } alt=""/></a>
          </li>
          <li>
            <a target="_blank" href="https://slack.minio.io">Ask for help  <img src={ iconHelp }  alt=""/></a>
          </li>
          <li>
            <a href="" onClick={ logoutFunc }>Sign Out  <img src={ iconOff }  alt=""/></a>
          </li>
        </Dropdown.Menu>
      </Dropdown>
    </nav>
  )
}

export default connect(state => state)(BrowserDropdown)
