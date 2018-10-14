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
import Alert from "../alert/Alert"
import * as actionsAlert from "../alert/actions"
import web from "../web"
import { Redirect } from "react-router-dom"

import loginImg from "../../img/login-img.svg"

export class Login extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      accessKey: "",
      secretKey: ""
    }
  }

  // Handle field changes
  accessKeyChange(e) {
    this.setState({
      accessKey: e.target.value
    })
  }

  secretKeyChange(e) {
    this.setState({
      secretKey: e.target.value
    })
  }

  handleSubmit(event) {
    event.preventDefault()
    const { showAlert, history } = this.props
    let message = ""
    if (this.state.accessKey === "") {
      message = "Access Key cannot be empty"
    }
    if (this.state.secretKey === "") {
      message = "Secret Key cannot be empty"
    }
    if (message) {
      showAlert("danger", message)
      return
    }
    web
      .Login({
        username: this.state.accessKey,
        password: this.state.secretKey
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
      <React.Fragment>
        {alertBox}
        <div className="login">
          <form className="login__form" onSubmit={this.handleSubmit.bind(this)}>
            <div className="form-item form-item--centered">
              <input
                type="text"
                value={this.state.accessKey}
                onChange={this.accessKeyChange.bind(this)}
                className="form-item__input"
                label="Access Key"
                id="accessKey"
                name="username"
                spellCheck="false"
                required="required"
                placeholder="Access Key"
                autoComplete="off"
              />
            </div>

            <div className="form-item form-item--centered">
              <input
                value={this.state.secretKey}
                onChange={this.secretKeyChange.bind(this)}
                className="form-item__input"
                label="Secret Key"
                id="secretKey"
                name="password"
                type="password"
                spellCheck="false"
                required="required"
                placeholder="Secret Key"
                autoComplete="off"
              />
            </div>

            <button className="button button--dark button--block" type="submit">
              Sign In
            </button>
          </form>

          <div className="login__texts">
            <h2>Minio</h2>
            <h3>Object Storage</h3>
            <p>
              Morbi leo risus, porta ac consectetur acvestibulum at erosonec id
              elit non mi porta gravida at eget metus. Donec sed odio dui.
              Maecenas faucibus mollis interdum.{" "}
            </p>
            <div className="login_img">
              <img src={loginImg} alt="" />
            </div>
          </div>
        </div>
      </React.Fragment>
    )
  }
}

const mapDispatchToProps = dispatch => {
  return {
    showAlert: (type, message) =>
      dispatch(actionsAlert.set({ type: type, message: message })),
    clearAlert: () => dispatch(actionsAlert.clear())
  }
}

export default connect(state => state, mapDispatchToProps)(Login)
