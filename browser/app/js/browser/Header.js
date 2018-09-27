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
import Path from "../objects/Path"
import web from "../web"
import { minioBrowserPrefix } from "../constants"
import { connect } from "react-redux"
import * as actionsCommon from "./actions"
import history from "../history"

export class Header extends React.Component {
  logout(e) {
    e.preventDefault()
    web.Logout()
    history.replace("/login")
  }
  render() {
    const loggedIn = web.LoggedIn()
    const { toggleSidebar } = this.props

    return (
      <header className="header">
        <div className="sidebar-toggle" onClick={toggleSidebar} />

        <Path />

        {loggedIn ? (
          <a onClick={this.logout} className="sign-out" />
        ) : (
          <a className="guest-login" href={minioBrowserPrefix + "/login"}>
            Login
          </a>
        )}
      </header>
    )
  }
}

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

export default connect(mapStateToProps, mapDispatchToProps)(Header)
