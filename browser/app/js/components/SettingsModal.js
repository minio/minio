/*
 * Minio Cloud Storage (C) 2016 Minio, Inc.
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
        <a className="close close--dark" onClick={ this.hideSettings.bind(this) }>×</a>
        <ModalHeader>
          Change Password
        </ModalHeader>
        <ModalBody>
          <div className="form-group">
            <label className="form-group__label">
              Access key
            </label>
            <input type="text"
              className="form-group__field"
              id="accessKey"
              name="accesskey"
              value={ settings.accessKey }
              onChange={ this.accessKeyChange.bind(this) }
              readonly={ settings.keysReadOnly } />
            <i className="form-group__bar"></i>
          </div>
          <div className="form-group">
            <label className="form-group__label">
              Secret key
            </label>
            <div className="form-group__wrap">
              <input type={ settings.secretKeyVisible ? "text" : "password" }
                className="form-group__field"
                id="secretKey"
                name="secretKey"
                value={ settings.secretKey }
                onChange={ this.secretKeyChange.bind(this) }
                readonly={ settings.keysReadOnly } />
              <div onClick={ this.secretKeyVisible.bind(this, !settings.secretKeyVisible) } className="form-group__addon">
                <i className={ "form-group__toggle-type" + (settings.secretKeyVisible ? " toggled" : "") }></i>
              </div>
              <i className="form-group__bar"></i>
            </div>
          </div>
        </ModalBody>
        <div className="modal-footer">
          <button className={ "btn btn--link " + (settings.keysReadOnly ? "hidden" : "") } onClick={ this.generateAuth.bind(this) }>
            Generate
          </button>
          <button className={ "btn btn--link " + (settings.keysReadOnly ? "hidden" : "") } onClick={ this.setAuth.bind(this) }>
            Update
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
