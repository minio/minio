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
import classNames from "classnames"
import { connect } from "react-redux"
import logo from "../../img/logo.svg"
import * as actionsCommon from "./actions"

export const MobileHeader = ({ sidebarOpen, toggleSidebar }) => (
  <header className="fe-header-mobile hidden-lg hidden-md">
    <div
      id="sidebar-toggle"
      className={
        "feh-trigger " +
        classNames({
          "feht-toggled": sidebarOpen
        })
      }
      onClick={e => {
        e.stopPropagation()
        toggleSidebar()
      }}
    >
      <div className="feht-lines">
        <div className="top" />
        <div className="center" />
        <div className="bottom" />
      </div>
    </div>
    <img className="mh-logo" src={logo} alt="" />
  </header>
)

const mapStateToProps = state => {
  return {
    sidebarOpen: state.browser.sidebarOpen
  }
}

const mapDispatchToProps = dispatch => {
  return {
    toggleSidebar: () => dispatch(actionsCommon.toggleSidebar())
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(MobileHeader)
