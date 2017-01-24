/*
 * Minio Browser (C) 2016 Minio, Inc.
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

import React from 'react'
import connect from 'react-redux/lib/components/connect'
import * as actions from '../actions'

import Tooltip from 'react-bootstrap/lib/Tooltip'
import Modal from 'react-bootstrap/lib/Modal'
import ModalBody from 'react-bootstrap/lib/ModalBody'
import ModalHeader from 'react-bootstrap/lib/ModalHeader'
import OverlayTrigger from 'react-bootstrap/lib/OverlayTrigger'
import InputGroup from './InputGroup'

class SettingsModal extends React.Component {

  // When the settings are shown, it loads the access key and secret key.
  componentWillMount() {
    const {web, dispatch} = this.props
    const {serverInfo} = this.props

    let accessKeyEnv = ''
    let secretKeyEnv = ''
    // Check environment variables first. They may or may not have been
    // loaded already; they load in Browse#componentDidMount.
    if (serverInfo.envVars) {
      serverInfo.envVars.forEach(envVar => {
        let keyVal = envVar.split('=')
        if (keyVal[0] == 'MINIO_ACCESS_KEY') {
          accessKeyEnv = keyVal[1]
        } else if (keyVal[0] == 'MINIO_SECRET_KEY') {
          secretKeyEnv = keyVal[1]
        }
      })
    }
    if (accessKeyEnv != '' || secretKeyEnv != '') {
      dispatch(actions.setSettings({
        accessKey: accessKeyEnv,
        secretKey: secretKeyEnv,
        keysReadOnly: true
      }))
    } else {
      web.GetAuth()
        .then(data => {
          dispatch(actions.setSettings({
            accessKey: data.accessKey,
            secretKey: data.secretKey
          }))
        })
    }
  }

  // When they are re-hidden, the keys are unloaded from memory.
  componentWillUnmount() {
    const {dispatch} = this.props

    dispatch(actions.setSettings({
      accessKey: '',
      secretKey: '',
      secretKeyVisible: false
    }))
    dispatch(actions.hideSettings())
  }

  // Handle field changes from inside the modal.
  accessKeyChange(e) {
    const {dispatch} = this.props
    dispatch(actions.setSettings({
      accessKey: e.target.value
    }))
  }

  secretKeyChange(e) {
    const {dispatch} = this.props
    dispatch(actions.setSettings({
      secretKey: e.target.value
    }))
  }

  secretKeyVisible(secretKeyVisible) {
    const {dispatch} = this.props
    dispatch(actions.setSettings({
      secretKeyVisible
    }))
  }

  // Save the auth params and set them.
  setAuth(e) {
    e.preventDefault()
    const {web, dispatch} = this.props

    let accessKey = document.getElementById('accessKey').value
    let secretKey = document.getElementById('secretKey').value
    web.SetAuth({
      accessKey,
      secretKey
    })
      .then(data => {
        dispatch(actions.setSettings({
          accessKey: '',
          secretKey: '',
          secretKeyVisible: false
        }))
        dispatch(actions.hideSettings())
        dispatch(actions.showAlert({
          type: 'success',
          message: 'Changed credentials'
        }))
      })
      .catch(err => {
        dispatch(actions.setSettings({
          accessKey: '',
          secretKey: '',
          secretKeyVisible: false
        }))
        dispatch(actions.hideSettings())
        dispatch(actions.showAlert({
          type: 'danger',
          message: err.message
        }))
      })
  }

  generateAuth(e) {
    e.preventDefault()
    const {dispatch} = this.props

    web.GenerateAuth()
      .then(data => {
        dispatch(actions.setSettings({
          secretKeyVisible: true
        }))
        dispatch(actions.setSettings({
          accessKey: data.accessKey,
          secretKey: data.secretKey
        }))
      })
  }

  hideSettings(e) {
    e.preventDefault()

    const {dispatch} = this.props
    dispatch(actions.hideSettings())
  }

  render() {
    let {settings} = this.props

    return (
      <Modal bsSize="sm" animation={ false } show={ true }>
        <ModalHeader>
          Change Password
        </ModalHeader>
        <ModalBody className="m-t-20">
          <InputGroup value={ settings.accessKey }
            onChange={ this.accessKeyChange.bind(this) }
            id="accessKey"
            label="Access Key"
            name="accesskey"
            type="text"
            spellCheck="false"
            required="required"
            autoComplete="false"
            align="ig-left"
            readonly={ settings.keysReadOnly }></InputGroup>
          <i onClick={ this.secretKeyVisible.bind(this, !settings.secretKeyVisible) } className={ "toggle-password fa fa-eye " + (settings.secretKeyVisible ? "toggled" : "") } />
          <InputGroup value={ settings.secretKey }
            onChange={ this.secretKeyChange.bind(this) }
            id="secretKey"
            label="Secret Key"
            name="accesskey"
            type={ settings.secretKeyVisible ? "text" : "password" }
            spellCheck="false"
            required="required"
            autoComplete="false"
            align="ig-left"
            readonly={ settings.keysReadOnly }></InputGroup>
        </ModalBody>
        <div className="modal-footer">
          <button className={ "btn btn-primary " + (settings.keysReadOnly ? "hidden" : "") } onClick={ this.generateAuth.bind(this) }>
            Generate
          </button>
          <button href="" className={ "btn btn-success " + (settings.keysReadOnly ? "hidden" : "") } onClick={ this.setAuth.bind(this) }>
            Update
          </button>
          <button href="" className="btn btn-link" onClick={ this.hideSettings.bind(this) }>
            Cancel
          </button>
        </div>
      </Modal>
    )
  }
}

export default connect(state => {
  return {
    web: state.web,
    settings: state.settings,
    serverInfo: state.serverInfo
  }
})(SettingsModal)
