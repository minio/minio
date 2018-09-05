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
import web from "../web"
import * as alertActions from "../alert/actions"

export class ChangePassword extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      accessKey: "",
      secretKey: "",
      keysReadOnly: false
    }
  }
  // When its shown, it loads the access key and secret key.
  componentWillMount() {
    const { serverInfo } = this.props

    // Check environment variables first.
    if (serverInfo.info.isEnvCreds || serverInfo.info.isWorm) {
      this.setState({
        accessKey: "xxxxxxxxx",
        secretKey: "xxxxxxxxx",
        keysReadOnly: true
      })
    } else {
      web.GetAuth().then(data => {
        this.setState({
          accessKey: data.accessKey,
          secretKey: data.secretKey
        })
      })
    }
  }

  // Handle field changes from inside the modal.
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

  secretKeyVisible(secretKeyVisible) {
    this.setState({
      secretKeyVisible
    })
  }

  // Save the auth params and set them.
  setAuth() {
    const { showAlert } = this.props
    const accessKey = this.state.accessKey
    const secretKey = this.state.secretKey
    web
      .SetAuth({
        accessKey,
        secretKey
      })
      .then(data => {
        showAlert({
          type: "success",
          message: "Changed credentials"
        })
      })
      .catch(err => {
        showAlert({
          type: "danger",
          message: err.message
        })
      })

    this.props.hideChangePassword(this.props.value)
  }

  generateAuth() {
    web.GenerateAuth().then(data => {
      this.setState({
        accessKey: data.accessKey,
        secretKey: data.secretKey,
        secretKeyVisible: true
      })
    })
  }

  render() {
    const { hideChangePassword } = this.props

    return (
      <React.Fragment>
        <div className="settings__panel__inner">
          <div className="form-item form-item--dark">
            <label
              className="form-item__label"
              htmlFor="change-password-access-key"
            >
              Access Key
            </label>
            <input
              type="text"
              className="form-item__input"
              id="change-password-access-key"
              value={this.state.accessKey}
              onChange={this.accessKeyChange.bind(this)}
              spellCheck="false"
              required="required"
              autoComplete="false"
              readOnly={this.state.keysReadOnly}
            />
          </div>

          <div className="form-item form-item--dark">
            <label
              className="form-item__label"
              htmlFor="change-password-secret-key"
            >
              Secret Key
            </label>
            <input
              type="text"
              className="form-item__input"
              id="change-password-secret-key"
              value={this.state.secretKey}
              onChange={this.secretKeyChange.bind(this)}
              spellCheck="false"
              required="required"
              autoComplete="false"
              readOnly={this.state.keysReadOnly}
            />
          </div>
        </div>
        <div className="settings__panel__footer">
          <button
            id="generate-keys"
            className={
              "button button--dark " + (this.state.keysReadOnly ? "hidden" : "")
            }
            onClick={this.generateAuth.bind(this)}
          >
            Generate
          </button>
          <button
            id="update-keys"
            className={
              "button button--dark " + (this.state.keysReadOnly ? "hidden" : "")
            }
            onClick={this.setAuth.bind(this)}
          >
            Update
          </button>
          <button
            id="cancel-change-password"
            className="button button--dark"
            onClick={hideChangePassword}
          >
            Cancel
          </button>
        </div>
      </React.Fragment>
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
    showAlert: alert => dispatch(alertActions.set(alert))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(ChangePassword)
