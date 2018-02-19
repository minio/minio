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

import {
  Tooltip,
  Modal,
  ModalBody,
  ModalHeader,
  OverlayTrigger
} from "react-bootstrap"
import InputGroup from "./InputGroup"

export class ChangePasswordModal extends React.Component {
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
    if (serverInfo.info.isEnvCreds) {
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
  setAuth(e) {
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
  }

  generateAuth(e) {
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
      <Modal bsSize="sm" animation={false} show={true}>
        <ModalHeader>Change Password</ModalHeader>
        <ModalBody className="m-t-20">
          <InputGroup
            value={this.state.accessKey}
            onChange={this.accessKeyChange.bind(this)}
            id="accessKey"
            label="Access Key"
            name="accesskey"
            type="text"
            spellCheck="false"
            required="required"
            autoComplete="false"
            align="ig-left"
            readonly={this.state.keysReadOnly}
          />
          <i
            onClick={this.secretKeyVisible.bind(
              this,
              !this.state.secretKeyVisible
            )}
            className={
              "toggle-password fa fa-eye " +
              (this.state.secretKeyVisible ? "toggled" : "")
            }
          />
          <InputGroup
            value={this.state.secretKey}
            onChange={this.secretKeyChange.bind(this)}
            id="secretKey"
            label="Secret Key"
            name="accesskey"
            type={this.state.secretKeyVisible ? "text" : "password"}
            spellCheck="false"
            required="required"
            autoComplete="false"
            align="ig-left"
            readonly={this.state.keysReadOnly}
          />
        </ModalBody>
        <div className="modal-footer">
          <button
            id="generate-keys"
            className={
              "btn btn-primary " + (this.state.keysReadOnly ? "hidden" : "")
            }
            onClick={this.generateAuth.bind(this)}
          >
            Generate
          </button>
          <button
            id="update-keys"
            className={
              "btn btn-success " + (this.state.keysReadOnly ? "hidden" : "")
            }
            onClick={this.setAuth.bind(this)}
          >
            Update
          </button>
          <button
            id="cancel-change-password"
            className="btn btn-link"
            onClick={hideChangePassword}
          >
            Cancel
          </button>
        </div>
      </Modal>
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

export default connect(mapStateToProps, mapDispatchToProps)(ChangePasswordModal)
