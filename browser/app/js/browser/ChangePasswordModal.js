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
import { connect } from "react-redux"
import web from "../web"
import * as alertActions from "../alert/actions"
import { getRandomAccessKey, getRandomSecretKey } from "../utils"
import jwtDecode from "jwt-decode"
import classNames from "classnames"

import { Modal, ModalBody, ModalHeader } from "react-bootstrap"
import InputGroup from "./InputGroup"
import { ACCESS_KEY_MIN_LENGTH, SECRET_KEY_MIN_LENGTH } from "../constants"

export class ChangePasswordModal extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      currentAccessKey: "",
      currentSecretKey: "",
      currentSecretKeyVisible: false,
      newAccessKey: "",
      newSecretKey: "",
      newSecretKeyVisible: false
    }
  }
  // When its shown, it loads the access key from JWT token
  componentWillMount() {
    const token = jwtDecode(web.GetToken())
    this.setState({
      currentAccessKey: token.sub,
      newAccessKey: token.sub
    })
  }

  // Save the auth params and set them.
  setAuth(e) {
    const { showAlert } = this.props

    if (this.canUpdateCredentials()) {
      const currentAccessKey = this.state.currentAccessKey
      const currentSecretKey = this.state.currentSecretKey
      const newAccessKey = this.state.newAccessKey
      const newSecretKey = this.state.newSecretKey
      web
        .SetAuth({
          currentAccessKey,
          currentSecretKey,
          newAccessKey,
          newSecretKey
        })
        .then(data => {
          showAlert({
            type: "success",
            message: "Credentials updated successfully."
          })
        })
        .catch(err => {
          showAlert({
            type: "danger",
            message: err.message
          })
        })
    }
  }

  generateAuth(e) {
    const { serverInfo } = this.props
    this.setState({
      newSecretKey: getRandomSecretKey(),
      newSecretKeyVisible: true
    })
  }

  canChangePassword() {
    const { serverInfo } = this.props
    // Password change is not allowed for temporary users(STS)
    if(serverInfo.userInfo.isTempUser) {
      return false
    }

    // Password change is only allowed for regular users
    if (!serverInfo.userInfo.isIAMUser) {
      return false
    }

    return true
  }

  canUpdateCredentials() {
    return (
      this.state.currentAccessKey.length > 0 &&
      this.state.currentSecretKey.length > 0 &&
      this.state.newAccessKey.length >= ACCESS_KEY_MIN_LENGTH &&
      this.state.newSecretKey.length >= SECRET_KEY_MIN_LENGTH
    )
  }

  render() {
    const { hideChangePassword, serverInfo } = this.props
    const allowChangePassword = this.canChangePassword()

    if (!allowChangePassword) {
      return (
        <Modal bsSize="sm" animation={false} show={true}>
          <ModalHeader>Change Password</ModalHeader>
          <ModalBody>
            Credentials of this user cannot be updated through MinIO Browser.
          </ModalBody>
          <div className="modal-footer">
            <button
              id="cancel-change-password"
              className="btn btn-link"
              onClick={hideChangePassword}
            >
              Close
            </button>
          </div>
        </Modal>
      )
    }

    return (
      <Modal bsSize="sm" animation={false} show={true}>
        <ModalHeader>Change Password</ModalHeader>
        <ModalBody className="m-t-20">
          <div className="has-toggle-password">
            <InputGroup
              value={this.state.currentAccessKey}
              id="currentAccessKey"
              label="Current Access Key"
              name="currentAccesskey"
              type="text"
              spellCheck="false"
              required="required"
              autoComplete="false"
              align="ig-left"
              readonly={true}
            />

            <i
              onClick={() => {
                this.setState({
                  currentSecretKeyVisible: !this.state.currentSecretKeyVisible
                })
              }}
              className={
                "toggle-password fas fa-eye " +
                (this.state.currentSecretKeyVisible ? "toggled" : "")
              }
            />
            <InputGroup
              value={this.state.currentSecretKey}
              onChange={e => {
                this.setState({ currentSecretKey: e.target.value })
              }}
              id="currentSecretKey"
              label="Current Secret Key"
              name="currentSecretKey"
              type={this.state.currentSecretKeyVisible ? "text" : "password"}
              spellCheck="false"
              required="required"
              autoComplete="false"
              align="ig-left"
            />
          </div>

          <div className="has-toggle-password m-t-30">
            <i
              onClick={() => {
                this.setState({
                  newSecretKeyVisible: !this.state.newSecretKeyVisible
                })
              }}
              className={
                "toggle-password fas fa-eye " +
                (this.state.newSecretKeyVisible ? "toggled" : "")
              }
            />
            <InputGroup
              value={this.state.newSecretKey}
              onChange={e => {
                this.setState({ newSecretKey: e.target.value })
              }}
              id="newSecretKey"
              label="New Secret Key"
              name="newSecretKey"
              type={this.state.newSecretKeyVisible ? "text" : "password"}
              spellCheck="false"
              required="required"
              autoComplete="false"
              align="ig-left"
              onChange={e => {
                this.setState({ newSecretKey: e.target.value })
              }}
            />
          </div>
        </ModalBody>
        <div className="modal-footer">
          <button
            id="generate-keys"
            className={"btn btn-primary"}
            onClick={this.generateAuth.bind(this)}
          >
            Generate
          </button>
          <button
            id="update-keys"
            className={classNames({
              btn: true,
              "btn-success": this.canUpdateCredentials()
            })}
            disabled={!this.canUpdateCredentials()}
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

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(ChangePasswordModal)
