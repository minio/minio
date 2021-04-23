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
import ClickOutHandler from "react-onclickout"
import { connect } from "react-redux"

import logo from "../../img/logo.svg"
import BucketSearch from "../buckets/BucketSearch"
import BucketList from "../buckets/BucketList"
import Host from "./Host"
import * as actionsCommon from "./actions"
import web from "../web"

export const SideBar = ({ sidebarOpen, clickOutside }) => {
  const onClickOut = e => {
    if (e.target.classList.contains("feh-trigger")) {
      return
    }
    clickOutside()
  }
  return (
    <ClickOutHandler onClickOut={onClickOut}>
      <div
        className={classNames({
          "fe-sidebar": true,
          toggled: sidebarOpen
        })}
      >
        <div className="fes-header clearfix hidden-sm hidden-xs">
          <img src={logo} alt="" />
          <h2>MinIO Browser</h2>
        </div>
        <div className="fes-list">
          {web.LoggedIn() && <BucketSearch />}
          <BucketList />
        </div>
        <Host />
      </div>
    </ClickOutHandler>
  )
}

const mapStateToProps = state => {
  return {
    sidebarOpen: state.browser.sidebarOpen
  }
}

const mapDispatchToProps = dispatch => {
  return {
    clickOutside: () => dispatch(actionsCommon.closeSidebar())
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(SideBar)
