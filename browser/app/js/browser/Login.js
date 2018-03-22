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
import logo from "../../img/logo.svg"
import Alert from "../alert/Alert"
import * as actionsAlert from "../alert/actions"
import web from "../web"
import { Redirect } from "react-router-dom"

export class Login extends React.Component {
  handleSubmit(event) {
    event.preventDefault()
    const { showAlert, history } = this.props
    let message = ""
    if (!document.getElementById("accessKey").value) {
      message = "Access Key cannot be empty"
    }
    if (!document.getElementById("secretKey").value) {
      message = "Secret Key cannot be empty"
    }
    if (message) {
      showAlert("danger", message)
      return
    }
    web
      .Login({
        username: document.getElementById("accessKey").value,
        password: document.getElementById("secretKey").value
      })
      .then(res => {
        history.push("/")
      })
      .catch(e => {
        showAlert("danger", e.message)
      })
  }

  componentWillMount() {
    const { clearAlert } = this.props
    // Clear out any stale message in the alert of previous page
    clearAlert()
    document.body.classList.add("is-guest")
  }

  componentWillUnmount() {
    document.body.classList.remove("is-guest")
  }

  render() {
    const { clearAlert, alert } = this.props
    if (web.LoggedIn()) {
      return <Redirect to={"/"} />
    }
    let alertBox = <Alert {...alert} onDismiss={clearAlert} />
    // Make sure you don't show a fading out alert box on the initial web-page load.
    if (!alert.message) alertBox = ""
    return (
      <div className="login">
        {alertBox}
        <div className="login__inner">
          <div className="login__header">
            <img className="login__logo" src={logo} alt="" />
            <div className="login__host">{window.location.host}</div>
          </div>
          <form className="login__form" onSubmit={this.handleSubmit.bind(this)}>
            <div className="form-group">
              <input
                placeholder="Access Key"
                type="text"
                id="accessKey"
                className="form-group__field"
                spellCheck="false"
                required="required"
              />
              <i className="form-group__helper" />
            </div>
            <div className="form-group">
              <input
                placeholder="Secret Key"
                type="password"
                id="secretKey"
                className="form-group__field"
                spellCheck="false"
                required="required"
              />
              <i className="form-group__helper" />
            </div>
            <button className="login__btn" type="submit" />
          </form>
        </div>
      </div>
    )
  }
}

const mapDispatchToProps = dispatch => {
  return {
    showAlert: (type, message) =>
      dispatch(
        actionsAlert.set({
          type: type,
          message: message
        })
      ),
    clearAlert: () => dispatch(actionsAlert.clear())
  }
}

export default connect(state => state, mapDispatchToProps)(Login)
