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
