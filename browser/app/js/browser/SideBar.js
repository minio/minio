/*
 * Minio Cloud Storage (C) 2016, 2018 Minio, Inc.
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
import ClickOutHandler from "react-onclickout"
import { connect } from "react-redux"

import logo from "../../img/logo.svg"
import Dropdown from "react-bootstrap/lib/Dropdown"
import BucketSearch from "../buckets/BucketSearch"
import BucketList from "../buckets/BucketList"
import Host from "./Host"
import * as actionsCommon from "./actions"
import web from "../web"

export const SideBar = ({ sidebarOpen, clickOutside }) => {
  return (
    <ClickOutHandler onClickOut={clickOutside}>
      <div
        className={classNames({
          "fe-sidebar": true,
          toggled: sidebarOpen
        })}
      >
        <div className="fes-header clearfix hidden-sm hidden-xs">
          <img src={logo} alt="" />
          <h2>Minio Browser</h2>
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

export default connect(mapStateToProps, mapDispatchToProps)(SideBar)
