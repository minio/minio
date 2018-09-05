/*
 * Minio Cloud Storage (C) 2016, 2017, 2018 Minio, Inc.
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
import { connect } from "react-redux"
import * as browserActions from "./actions"
import web from "../web"
import history from "../history"
import About from "./About"
import ChangePassword from "./ChangePassword"
import classNames from "classnames"
import ClickOutHandler from "react-onclickout"

export class SettingsMenu extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      aboutActive: false,
      changePasswordActive: false,
      panelExpand: false,

      panelEnters: false,
      panelClosing: false,

      expandMenu: false,
      hideSettingsNav: false
    }
  }

  showModal(modal, e) {
    e.preventDefault()
    this.setState({
      panelExpand: true,
      hideSettingsNav: true
    })

    if (modal === "about") {
      this.setState({
        aboutActive: true
      })
    } else {
      this.setState({
        changePasswordActive: true
      })
    }

    setTimeout(() => {
      this.setState({
        panelEnters: true
      })
    }, 400)
  }

  hideModal() {
    this.setState({
      panelEnters: false
    })

    setTimeout(() => {
      this.setState({
        aboutActive: false,
        changePasswordActive: false,
        panelExpand: false
      })
    }, 200)

    setTimeout(() => {
      this.setState({
        hideSettingsNav: false
      })
    }, 600)
  }

  showChangePassword(e) {
    e.preventDefault()
    this.setState({
      changePasswordActive: true,
      panelExpand: true,
      hideSettingsNav: true
    })
  }
  hideChangePassword() {
    this.setState({
      changePasswordActive: false
    })
  }
  componentDidMount() {
    const { fetchServerInfo } = this.props
    fetchServerInfo()
  }
  fullScreen(e) {
    e.preventDefault()
    let el = document.documentElement
    if (el.requestFullscreen) {
      el.requestFullscreen()
    }
    if (el.mozRequestFullScreen) {
      el.mozRequestFullScreen()
    }
    if (el.webkitRequestFullscreen) {
      el.webkitRequestFullscreen()
    }
    if (el.msRequestFullscreen) {
      el.msRequestFullscreen()
    }
  }
  logout(e) {
    e.preventDefault()
    web.Logout()
    history.replace("/login")
  }
  render() {
    const { serverInfo, closeSettingsMenu } = this.props

    return (
      <ClickOutHandler
        onClickOut={
          this.state.panelExpand ? this.hideModal.bind(this) : closeSettingsMenu
        }
      >
        <div
          className={classNames({
            settings__menu: true,
            "settings__menu--expand": this.state.panelExpand
          })}
        >
          <nav
            className={classNames({
              settings__nav: true,
              "settings__nav--hidden": this.state.hideSettingsNav
            })}
          >
            <a target="_blank" href="https://github.com/minio/minio">
              GitHub
            </a>
            <a href="" onClick={this.fullScreen}>
              Fullscreen
            </a>
            <a target="_blank" href="https://docs.minio.io/">
              Documentation
            </a>
            <a target="_blank" href="https://slack.minio.io">
              Ask for help
            </a>
            <a
              href=""
              id="show-about"
              onClick={this.showModal.bind(this, "about")}
            >
              About
            </a>
            <a href="" onClick={this.showModal.bind(this, "changePassword")}>
              Change Password
            </a>
            <a href="" id="logout" onClick={this.logout}>
              Sign Out
            </a>
          </nav>

          <div
            className={classNames({
              settings__panel: true,
              "settings__panel--active": this.state.panelEnters
            })}
          >
            {this.state.aboutActive && (
              <About
                serverInfo={serverInfo}
                hideAbout={this.hideModal.bind(this)}
              />
            )}

            {this.state.changePasswordActive && (
              <ChangePassword
                serverInfo={serverInfo}
                hideChangePassword={this.hideModal.bind(this)}
              />
            )}
          </div>
        </div>
      </ClickOutHandler>
    )
  }
}

const mapStateToProps = state => {
  return {
    serverInfo: state.browser.serverInfo
  }
}

const mapDispatchToProps = dispatch => {
  return {
    fetchServerInfo: () => dispatch(browserActions.fetchServerInfo())
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(SettingsMenu)
