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

export const About = ({ serverInfo, hideAbout }) => {
  const { version, memory, platform, runtime } = serverInfo

  return (
    <React.Fragment>
      <div className="settings__panel__inner">
        <div className="about">
          <div className="about__item">
            <div className="about__title">Veron</div>
            <div className="about__value">{version}</div>
          </div>

          <div className="about__item">
            <div className="about__title">Memory</div>
            <div className="about__value">{memory}</div>
          </div>

          <div className="about__item">
            <div className="about__title">Platform</div>
            <div className="about__value">{platform}</div>
          </div>

          <div className="about__item">
            <div className="about__title">Runtime</div>
            <div className="about__value">{runtime}</div>
          </div>
        </div>
      </div>

      <div className="settings__footer">
        <button className="button button--dark" onClick={hideAbout}>
          Close
        </button>
      </div>
    </React.Fragment>
  )
}

export default About
