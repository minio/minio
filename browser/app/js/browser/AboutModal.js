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

import React from "react"
import { Modal } from "react-bootstrap"
import logo from "../../img/logo.svg"

export const AboutModal = ({ serverInfo, hideAbout }) => {
  const { version, platform, runtime } = serverInfo
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
          <a href="https://min.io" target="_blank">
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
