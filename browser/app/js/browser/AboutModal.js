/*
 * Minio Cloud Storage (C) 2018 Minio, Inc.
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

import React from "react"
import { Modal } from "react-bootstrap"
import logo from "../../img/logo.svg"

export const AboutModal = ({ serverInfo, hideAbout }) => {
  const { version, memory, platform, runtime } = serverInfo
  return (
    <Modal
      className="modal-about modal-dark"
      animation={false}
      show={true}
      onHide={hideAbout}
    >
      <button className="close" onClick={hideAbout}>
        <span>×</span>
      </button>
      <div className="ma-inner">
        <div className="mai-item hidden-xs">
          <a href="https://minio.io" target="_blank">
            <img className="maii-logo" src={logo} alt="" />
          </a>
        </div>
        <div className="mai-item">
          <ul className="maii-list">
            <li>
              <div>Version</div>
              <small>{version}</small>
            </li>
            <li>
              <div>Memory</div>
              <small>{memory}</small>
            </li>
            <li>
              <div>Platform</div>
              <small>{platform}</small>
            </li>
            <li>
              <div>Runtime</div>
              <small>{runtime}</small>
            </li>
          </ul>
        </div>
      </div>
    </Modal>
  )
}

export default AboutModal
