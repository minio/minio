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
    <Modal className="about" animation={false} show={true} onHide={hideAbout}>
      <i className="close" onClick={hideAbout} />
      <div className="about__content">
        <div className="about__logo hidden-xs">
          <img src={logo} alt="" />
        </div>
        <div className="about__info">
          <div className="about__item">
            <strong>Version</strong>
            <small>{version}</small>
          </div>
          <div className="about__item">
            <strong>Memory</strong>
            <small>{memory}</small>
          </div>
          <div className="about__item">
            <strong>Platform</strong>
            <small>{platform}</small>
          </div>
          <div className="about__item">
            <strong>Runtime</strong>
            <small>{runtime}</small>
          </div>
        </div>
      </div>
    </Modal>
  )
}

export default AboutModal
