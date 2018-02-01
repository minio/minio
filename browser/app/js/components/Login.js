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
import { connect } from "react-redux"
import classNames from "classnames"
import logo from "../../img/logo.svg"
import Alert from "./Alert"
import * as actionsAlert from "../actions/alert"
import InputGroup from "../components/InputGroup"
import { minioBrowserPrefix } from "../constants"
import web from "../web"
import { Redirect } from "react-router-dom"

export class Login extends React.Component {
  handleSubmit(event) {
    event.preventDefault()
    const { dispatch, history } = this.props
    let message = ""
    if (!document.getElementById("accessKey").value) {
      message = "Secret Key cannot be empty"
    }
    if (!document.getElementById("secretKey").value) {
      message = "Access Key cannot be empty"
    }
    if (message) {
      dispatch(
        actionsAlert.set({
          type: "danger",
          message
        })
      )
      return
    }
    web
      .Login({
        username: document.getElementById("accessKey").value,
        password: document.getElementById("secretKey").value
      })
      .then(res => {
        history.push(minioBrowserPrefix)
      })
      .catch(e => {
        dispatch(
          actionsAlert.set({
            type: "danger",
            message: e.message
          })
        )
      })
  }

  componentWillMount() {
    const { dispatch } = this.props
    // Clear out any stale message in the alert of previous page
    dispatch(actionsAlert.clear())
    document.body.classList.add("is-guest")
  }

  componentWillUnmount() {
    document.body.classList.remove("is-guest")
  }

  clearAlert() {
    const { dispatch } = this.props
    dispatch(actionsAlert.clear())
  }

  render() {
    const { alert } = this.props
    if (web.LoggedIn()) {
      return <Redirect to={minioBrowserPrefix} />
    }
    let alertBox = <Alert {...alert} onDismiss={this.clearAlert.bind(this)} />
    // Make sure you don't show a fading out alert box on the initial web-page load.
    if (!alert.message) alertBox = ""
    return (
      <div className="login">
        {alertBox}
        <div className="l-wrap">
          <form onSubmit={this.handleSubmit.bind(this)}>
            <InputGroup
              className="ig-dark"
              label="Access Key"
              id="accessKey"
              name="username"
              type="text"
              spellCheck="false"
              required="required"
              autoComplete="username"
            />
            <InputGroup
              className="ig-dark"
              label="Secret Key"
              id="secretKey"
              name="password"
              type="password"
              spellCheck="false"
              required="required"
              autoComplete="new-password"
            />
            <button className="lw-btn" type="submit">
              <i className="fa fa-sign-in" />
            </button>
          </form>
        </div>
        <div className="l-footer">
          <a className="lf-logo" href="">
            <img src={logo} alt="" />
          </a>
          <div className="lf-server">{window.location.host}</div>
        </div>
      </div>
    )
  }
}

export default connect(state => state)(Login)
