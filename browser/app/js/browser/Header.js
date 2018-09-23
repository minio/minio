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
import SettingsMenu from "./SettingsMenu"
import web from "../web"
import { minioBrowserPrefix } from "../constants"
import { connect } from "react-redux"
import classNames from "classnames"
import * as actionsCommon from "./actions"

export class Header extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      settingsActive: false,
      settingsClosing: false,
      settingsEnters: false
    }
  }

  openSettingsMenu() {
    this.setState({
      settingsClosing: false,
      settingsEnters: false,
      settingsActive: true
    })

    setTimeout(() => {
      this.setState({
        settingsEnters: true
      })
    }, 1)
  }

  closeSettingsMenu() {
    this.setState({
      settingsClosing: true
    })

    setTimeout(() => {
      this.setState({
        settingsEnters: false
      })
    }, 450)

    setTimeout(() => {
      this.setState({
        settingsActive: false
      })
    }, 750)
  }

  render() {
    const loggedIn = web.LoggedIn()
    const { toggleSidebar } = this.props

    return (
      <header className="header">
        <div className="sidebar-toggle" onClick={toggleSidebar} />

        <Path />

        {loggedIn ? (
          <div
            className={classNames({
              settings: true,
              "settings--active": this.state.settingsEnters,
              "settings--closing": this.state.settingsClosing
            })}
          >
            <i
              onClick={this.openSettingsMenu.bind(this)}
              className="settings__toggle"
            />

            {this.state.settingsActive && (
              <SettingsMenu
                closeSettingsMenu={this.closeSettingsMenu.bind(this)}
              />
            )}
          </div>
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
