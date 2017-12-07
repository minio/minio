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
import classNames from 'classnames'
import browserHistory from 'react-router/lib/browserHistory'
import humanize from 'humanize'
import Moment from 'moment'
import Modal from 'react-bootstrap/lib/Modal'
import ModalBody from 'react-bootstrap/lib/ModalBody'
import ModalHeader from 'react-bootstrap/lib/ModalHeader'
import Alert from 'react-bootstrap/lib/Alert'
import OverlayTrigger from 'react-bootstrap/lib/OverlayTrigger'
import Tooltip from 'react-bootstrap/lib/Tooltip'
import Dropdown from 'react-bootstrap/lib/Dropdown'
import MenuItem from 'react-bootstrap/lib/MenuItem'
import InputGroup from '../components/InputGroup'
import Dropzone from '../components/Dropzone'
import ObjectsList from '../components/ObjectsList'
import SideBar from '../components/SideBar'
import Path from '../components/Path'
import BrowserUpdate from '../components/BrowserUpdate'
import UploadModal from '../components/UploadModal'
import SettingsModal from '../components/SettingsModal'
import PolicyInput from '../components/PolicyInput'
import Policy from '../components/Policy'
import BrowserDropdown from '../components/BrowserDropdown'
import ConfirmModal from './ConfirmModal'
import logo from '../../img/logo.svg'
import * as actions from '../actions'
import * as utils from '../utils'
import * as mime from '../mime'
import { minioBrowserPrefix } from '../constants'
import CopyToClipboard from 'react-copy-to-clipboard'
import storage from 'local-storage-fallback'
import InfiniteScroll from 'react-infinite-scroller';

export default class Browse extends React.Component {
  componentDidMount() {
    const {web, dispatch, currentBucket} = this.props
    if (!web.LoggedIn()) return
    web.StorageInfo()
      .then(res => {
        let storageInfo = Object.assign({}, {
          total: res.storageInfo.Total,
          free: res.storageInfo.Free
        })
        storageInfo.used = storageInfo.total - storageInfo.free
        dispatch(actions.setStorageInfo(storageInfo))
        return web.ServerInfo()
      })
      .then(res => {
        let serverInfo = Object.assign({}, {
          version: res.MinioVersion,
          memory: res.MinioMemory,
          platform: res.MinioPlatform,
          runtime: res.MinioRuntime,
          info: res.MinioGlobalInfo
        })
        dispatch(actions.setServerInfo(serverInfo))
      })
      .catch(err => {
        dispatch(actions.showAlert({
          type: 'danger',
          message: err.message
        }))
      })
  }

  componentWillMount() {
    const {dispatch} = this.props
    // Clear out any stale message in the alert of Login page
    dispatch(actions.showAlert({
      type: 'danger',
      message: ''
    }))
    if (web.LoggedIn()) {
      web.ListBuckets()
        .then(res => {
          let buckets
          if (!res.buckets)
            buckets = []
          else
            buckets = res.buckets.map(bucket => bucket.name)
          if (buckets.length) {
            dispatch(actions.setBuckets(buckets))
            dispatch(actions.setVisibleBuckets(buckets))
            if (location.pathname === minioBrowserPrefix || location.pathname === minioBrowserPrefix + '/') {
              browserHistory.push(utils.pathJoin(buckets[0]))
            }
          }
        })
    }
    this.history = browserHistory.listen(({pathname}) => {
      let decPathname = decodeURI(pathname)
      if (decPathname === `${minioBrowserPrefix}/login`) return // FIXME: better organize routes and remove this
      if (!decPathname.endsWith('/'))
        decPathname += '/'
      if (decPathname === minioBrowserPrefix + '/') {
        return
      }
      let obj = utils.pathSlice(decPathname)
      if (!web.LoggedIn()) {
        dispatch(actions.setBuckets([obj.bucket]))
        dispatch(actions.setVisibleBuckets([obj.bucket]))
      }
      dispatch(actions.selectBucket(obj.bucket, obj.prefix))
    })
  }

  componentWillUnmount() {
    this.history()
  }

  selectBucket(e, bucket) {
    e.preventDefault()
    if (bucket === this.props.currentBucket) return
    browserHistory.push(utils.pathJoin(bucket))
  }

  searchBuckets(e) {
    e.preventDefault()
    let {buckets} = this.props
    this.props.dispatch(actions.setVisibleBuckets(buckets.filter(bucket => bucket.indexOf(e.target.value) > -1)))
  }

  listObjects() {
    const {dispatch} = this.props
    dispatch(actions.listObjects())
  }

  selectPrefix(e, prefix) {
    e.preventDefault()
    const {dispatch, currentPath, web, currentBucket} = this.props
    const encPrefix = encodeURI(prefix)
    if (prefix.endsWith('/') || prefix === '') {
      if (prefix === currentPath) return
      browserHistory.push(utils.pathJoin(currentBucket, encPrefix))
    } else {
      if (!web.LoggedIn()) {
        let url = `${window.location.origin}/minio/download/${currentBucket}/${encPrefix}?token=''`
        window.location = url
      } else {
        // Download the selected file.
        web.CreateURLToken()
          .then(res => {
            let url = `${window.location.origin}${minioBrowserPrefix}/download/${currentBucket}/${encPrefix}?token=${res.token}`
            window.location = url
          })
          .catch(err => dispatch(actions.showAlert({
            type: 'danger',
            message: err.message
          })))
      }
    }
  }

  makeBucket(e) {
    e.preventDefault()
    const bucketName = this.refs.makeBucketRef.value
    this.refs.makeBucketRef.value = ''
    const {web, dispatch} = this.props
    this.hideMakeBucketModal()
    web.MakeBucket({
      bucketName
    })
      .then(() => {
        dispatch(actions.addBucket(bucketName))
        dispatch(actions.selectBucket(bucketName))
      })
      .catch(err => dispatch(actions.showAlert({
        type: 'danger',
        message: err.message
      })))
  }

  hideMakeBucketModal() {
    const {dispatch} = this.props
    dispatch(actions.hideMakeBucketModal())
  }

  showMakeBucketModal(e) {
    e.preventDefault()
    const {dispatch} = this.props
    dispatch(actions.showMakeBucketModal())
  }

  showAbout(e) {
    e.preventDefault()
    const {dispatch} = this.props
    dispatch(actions.showAbout())
  }

  hideAbout(e) {
    e.preventDefault()
    const {dispatch} = this.props
    dispatch(actions.hideAbout())
  }

  toggleBucketDropdown(e) {
    const {dispatch, showBucketDropdown} = this.props
    if (showBucketDropdown) {
      dispatch(actions.hideBucketDropdown())
    } else {
      dispatch(actions.showBucketDropdown())
    }
  }

  showBucketPolicy(e) {
    e.preventDefault()
    const {dispatch} = this.props
    this.toggleBucketDropdown(e)
    dispatch(actions.showBucketPolicy())
  }

  hideBucketPolicy(e) {
    e.preventDefault()
    const {dispatch} = this.props
    dispatch(actions.hideBucketPolicy())
  }

  deleteBucket(e, bucket) {
    e.preventDefault()
    const {dispatch} = this.props
    this.toggleBucketDropdown(e)
    dispatch(actions.deleteBucket(bucket))
    browserHistory.push(`${minioBrowserPrefix}/`)
  }

  uploadFile(e) {
    e.preventDefault()
    const {dispatch, buckets, currentBucket} = this.props
    if (buckets.length === 0) {
      dispatch(actions.showAlert({
        type: 'danger',
        message: "Bucket needs to be created before trying to upload files."
      }))
      return
    }
    if (currentBucket === '') {
      dispatch(actions.showAlert({
        type: 'danger',
        message: "Please choose a bucket before trying to upload files."
      }))
      return
    }
    let file = e.target.files[0]
    e.target.value = null
    this.xhr = new XMLHttpRequest()
    dispatch(actions.uploadFile(file, this.xhr))
  }

  removeObject() {
    const {web, dispatch, currentPath, currentBucket, deleteConfirmation, checkedObjects} = this.props
    let objects = []
    if (checkedObjects.length > 0) {
      objects = checkedObjects.map(obj => `${currentPath}${obj}`)
    } else {
      objects = [deleteConfirmation.object]
    }

    web.RemoveObject({
      bucketname: currentBucket,
      objects: objects
    })
      .then(() => {
        this.hideDeleteConfirmation()
        if (checkedObjects.length > 0) {
          for (let i = 0; i < checkedObjects.length; i++) {
            dispatch(actions.removeObject(checkedObjects[i].replace(currentPath, '')))
          }
          dispatch(actions.checkedObjectsReset())
        } else {
          let delObject = deleteConfirmation.object.replace(currentPath, '')
          dispatch(actions.removeObject(delObject))
        }
      })
      .catch(e => dispatch(actions.showAlert({
        type: 'danger',
        message: e.message
      })))
  }

  hideAlert(e) {
    e.preventDefault()
    const {dispatch} = this.props
    dispatch(actions.hideAlert())
  }

  showDeleteConfirmation(e, object) {
    e.preventDefault()
    const {dispatch} = this.props
    dispatch(actions.showDeleteConfirmation(object))
  }

  hideDeleteConfirmation() {
    const {dispatch} = this.props
    dispatch(actions.hideDeleteConfirmation())
  }

  shareObject(e, object) {
    e.preventDefault()
    const {dispatch} = this.props
    // let expiry = 5 * 24 * 60 * 60 // 5 days expiry by default
    dispatch(actions.shareObject(object, 5, 0, 0))
  }

  hideShareObjectModal() {
    const {dispatch} = this.props
    dispatch(actions.hideShareObject())
  }

  dataType(name, contentType) {
    return mime.getDataType(name, contentType)
  }

  sortObjectsByName(e) {
    const {dispatch, objects, sortNameOrder} = this.props
    dispatch(actions.setObjects(utils.sortObjectsByName(objects, !sortNameOrder)))
    dispatch(actions.setSortNameOrder(!sortNameOrder))
  }

  sortObjectsBySize() {
    const {dispatch, objects, sortSizeOrder} = this.props
    dispatch(actions.setObjects(utils.sortObjectsBySize(objects, !sortSizeOrder)))
    dispatch(actions.setSortSizeOrder(!sortSizeOrder))
  }

  sortObjectsByDate() {
    const {dispatch, objects, sortDateOrder} = this.props
    dispatch(actions.setObjects(utils.sortObjectsByDate(objects, !sortDateOrder)))
    dispatch(actions.setSortDateOrder(!sortDateOrder))
  }

  logout(e) {
    const {web} = this.props
    e.preventDefault()
    web.Logout()
    browserHistory.push(`${minioBrowserPrefix}/login`)
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

  toggleSidebar(status) {
    this.props.dispatch(actions.setSidebarStatus(status))
  }

  hideSidebar(event) {
    let e = event || window.event;

    // Support all browsers.
    let target = e.srcElement || e.target;
    if (target.nodeType === 3) // Safari support.
      target = target.parentNode;

    let targetID = target.id;
    if (!(targetID === 'feh-trigger')) {
      this.props.dispatch(actions.setSidebarStatus(false))
    }
  }

  showSettings(e) {
    e.preventDefault()

    const {dispatch} = this.props
    dispatch(actions.showSettings())
  }

  showMessage() {
    const {dispatch} = this.props
    dispatch(actions.showAlert({
      type: 'success',
      message: 'Link copied to clipboard!'
    }))
    this.hideShareObjectModal()
  }

  selectTexts() {
    this.refs.copyTextInput.select()
  }

  handleExpireValue(targetInput, inc, object) {
    let value = this.refs[targetInput].value
    let maxValue = (targetInput == 'expireHours') ? 23 : (targetInput == 'expireMins') ? 59 : (targetInput == 'expireDays') ? 7 : 0
    value = isNaN(value) ? 0 : value

    // Use custom step count to support browser Edge
    if((inc === -1)) {
      if(value != 0) {
        value--
      }
    }
    else {
      if(value != maxValue) {
        value++
      }
    }
    this.refs[targetInput].value = value

    // Reset hours and mins when days reaches it's max value
    if (this.refs.expireDays.value == 7) {
      this.refs.expireHours.value = 0
      this.refs.expireMins.value = 0
    }
    if (this.refs.expireDays.value + this.refs.expireHours.value + this.refs.expireMins.value == 0) {
      this.refs.expireDays.value = 7
    }

    const {dispatch} = this.props
    dispatch(actions.shareObject(object, this.refs.expireDays.value, this.refs.expireHours.value, this.refs.expireMins.value))
  }

  checkObject(e, objectName) {
    const {dispatch} = this.props
    e.target.checked ? dispatch(actions.checkedObjectsAdd(objectName)) : dispatch(actions.checkedObjectsRemove(objectName))
  }

  downloadSelected() {
    const {dispatch, web} = this.props
    let req = {
      bucketName: this.props.currentBucket,
      objects: this.props.checkedObjects,
      prefix: this.props.currentPath
    }
    if (!web.LoggedIn()) {
      let requestUrl = location.origin + "/minio/zip?token=''"
      this.xhr = new XMLHttpRequest()
      dispatch(actions.downloadSelected(requestUrl, req, this.xhr))
    } else {
      web.CreateURLToken()
        .then(res => {
          let requestUrl = location.origin + minioBrowserPrefix + "/zip?token=" + res.token
          this.xhr = new XMLHttpRequest()
          dispatch(actions.downloadSelected(requestUrl, req, this.xhr))
        })
        .catch(err => dispatch(actions.showAlert({
          type: 'danger',
          message: err.message
        })))
    }
  }

  clearSelected() {
    const {dispatch} = this.props
    dispatch(actions.checkedObjectsReset())
  }

  render() {
    const {total, free} = this.props.storageInfo
    const {showMakeBucketModal, alert, sortNameOrder, sortSizeOrder, sortDateOrder, showAbout, showBucketPolicy, checkedObjects} = this.props
    const {version, memory, platform, runtime} = this.props.serverInfo
    const {sidebarStatus} = this.props
    const {showSettings} = this.props
    const {policies, currentBucket, currentPath} = this.props
    const {deleteConfirmation} = this.props
    const {shareObject} = this.props
    const {web, prefixWritable, istruncated} = this.props

    // Don't always show the SettingsModal. This is done here instead of in
    // SettingsModal.js so as to allow for #componentWillMount to handle
    // the loading of the settings.
    let settingsModal = showSettings ? <SettingsModal /> : <noscript></noscript>

    let alertBox = <Alert className={ classNames({
                     'alert': true,
                     'animated': true,
                     'fadeInDown': alert.show,
                     'fadeOutUp': !alert.show
                   }) } bsStyle={ alert.type } onDismiss={ this.hideAlert.bind(this) }>
                     <div className='text-center'>
                       { alert.message }
                     </div>
                   </Alert>
    // Make sure you don't show a fading out alert box on the initial web-page load.
    if (!alert.message)
      alertBox = ''

    let signoutTooltip = <Tooltip id="tt-sign-out">
                           Sign out
                         </Tooltip>
    let uploadTooltip = <Tooltip id="tt-upload-file">
                          Upload file
                        </Tooltip>
    let makeBucketTooltip = <Tooltip id="tt-create-bucket">
                              Create bucket
                            </Tooltip>
    let loginButton = ''
    let browserDropdownButton = ''
    let storageUsageDetails = ''

    let used = total - free
    let usedPercent = (used / total) * 100 + '%'
    let freePercent = free * 100 / total

    if (web.LoggedIn()) {
      browserDropdownButton = <BrowserDropdown fullScreenFunc={ this.fullScreen.bind(this) }
                                aboutFunc={ this.showAbout.bind(this) }
                                settingsFunc={ this.showSettings.bind(this) }
                                logoutFunc={ this.logout.bind(this) } />
    } else {
      loginButton = <a className='btn btn-danger' href={minioBrowserPrefix+'/login'}>Login</a>
    }

    if (web.LoggedIn()) {
      if (!(used === 0 && free === 0)) {
        storageUsageDetails = <div className="feh-usage">
          <div className="fehu-chart">
            <div style={ { width: usedPercent } }></div>
          </div>
          <ul>
            <li>
              <span>Used: </span>
              { humanize.filesize(total - free) }
            </li>
            <li className="pull-right">
              <span>Free: </span>
              { humanize.filesize(total - used) }
            </li>
          </ul>
        </div>
      }


    }

    let createButton = ''
    if (web.LoggedIn()) {
      createButton = <Dropdown dropup className="feb-actions" id="fe-action-toggle">
                       <Dropdown.Toggle noCaret className="feba-toggle">
                         <span><i className="fa fa-plus"></i></span>
                       </Dropdown.Toggle>
                       <Dropdown.Menu>
                         <OverlayTrigger placement="left" overlay={ uploadTooltip }>
                           <a href="#" className="feba-btn feba-upload">
                             <input type="file"
                               onChange={ this.uploadFile.bind(this) }
                               style={ { display: 'none' } }
                               id="file-input"></input>
                             <label htmlFor="file-input"> <i className="fa fa-cloud-upload"></i> </label>
                           </a>
                         </OverlayTrigger>
                         <OverlayTrigger placement="left" overlay={ makeBucketTooltip }>
                           <a href="#" className="feba-btn feba-bucket" onClick={ this.showMakeBucketModal.bind(this) }><i className="fa fa-hdd-o"></i></a>
                         </OverlayTrigger>
                       </Dropdown.Menu>
                     </Dropdown>

    } else {
      if (prefixWritable)
        createButton = <Dropdown dropup className="feb-actions" id="fe-action-toggle">
                         <Dropdown.Toggle noCaret className="feba-toggle">
                           <span><i className="fa fa-plus"></i></span>
                         </Dropdown.Toggle>
                         <Dropdown.Menu>
                           <OverlayTrigger placement="left" overlay={ uploadTooltip }>
                             <a href="#" className="feba-btn feba-upload">
                               <input type="file"
                                 onChange={ this.uploadFile.bind(this) }
                                 style={ { display: 'none' } }
                                 id="file-input"></input>
                               <label htmlFor="file-input"> <i className="fa fa-cloud-upload"></i> </label>
                             </a>
                           </OverlayTrigger>
                         </Dropdown.Menu>
                       </Dropdown>
    }

    return (
      <div className={ classNames({
                   'file-explorer': true,
                   'toggled': sidebarStatus
                 }) }>
        <SideBar searchBuckets={ this.searchBuckets.bind(this) }
          selectBucket={ this.selectBucket.bind(this) }
          clickOutside={ this.hideSidebar.bind(this) }
          showPolicy={ this.showBucketPolicy.bind(this) }
          deleteBucket={ this.deleteBucket.bind(this) }
          toggleBucketDropdown={ this.toggleBucketDropdown.bind(this) } />
        <div className="fe-body">
          <div className={ 'list-actions' + (classNames({
                             ' list-actions-toggled': checkedObjects.length > 0
                           })) }>
            <span className="la-label"><i className="fa fa-check-circle" /> { checkedObjects.length } Objects selected</span>
            <span className="la-actions pull-right"><button onClick={ this.downloadSelected.bind(this) }> Download all as zip </button></span>
            <span className="la-actions pull-right"><button onClick={ this.showDeleteConfirmation.bind(this) }> Delete selected </button></span>
            <i className="la-close fa fa-times" onClick={ this.clearSelected.bind(this) }></i>
          </div>
          <Dropzone>
            { alertBox }
            <header className="fe-header-mobile hidden-lg hidden-md">
              <div id="feh-trigger" className={ 'feh-trigger ' + (classNames({
                                                  'feht-toggled': sidebarStatus
                                                })) } onClick={ this.toggleSidebar.bind(this, !sidebarStatus) }>
                <div className="feht-lines">
                  <div className="top"></div>
                  <div className="center"></div>
                  <div className="bottom"></div>
                </div>
              </div>
              <img className="mh-logo" src={ logo } alt="" />
            </header>
            <header className="fe-header">
              <Path selectPrefix={ this.selectPrefix.bind(this) } />
              { storageUsageDetails }
              <ul className="feh-actions">
                <BrowserUpdate />
                { loginButton }
                { browserDropdownButton }
              </ul>
            </header>
            <div className="feb-container">
              <header className="fesl-row" data-type="folder">
                <div className="fesl-item fesl-item-icon"></div>
                <div className="fesl-item fesl-item-name" onClick={ this.sortObjectsByName.bind(this) } data-sort="name">
                  Name
                  <i className={ classNames({
                                   'fesli-sort': true,
                                   'fa': true,
                                   'fa-sort-alpha-desc': sortNameOrder,
                                   'fa-sort-alpha-asc': !sortNameOrder
                                 }) } />
                </div>
                <div className="fesl-item fesl-item-size" onClick={ this.sortObjectsBySize.bind(this) } data-sort="size">
                  Size
                  <i className={ classNames({
                                   'fesli-sort': true,
                                   'fa': true,
                                   'fa-sort-amount-desc': sortSizeOrder,
                                   'fa-sort-amount-asc': !sortSizeOrder
                                 }) } />
                </div>
                <div className="fesl-item fesl-item-modified" onClick={ this.sortObjectsByDate.bind(this) } data-sort="last-modified">
                  Last Modified
                  <i className={ classNames({
                                   'fesli-sort': true,
                                   'fa': true,
                                   'fa-sort-numeric-desc': sortDateOrder,
                                   'fa-sort-numeric-asc': !sortDateOrder
                                 }) } />
                </div>
                <div className="fesl-item fesl-item-actions"></div>
              </header>
            </div>
            <div className="feb-container">
              <InfiniteScroll loadMore={ this.listObjects.bind(this) }
                hasMore={ istruncated }
                useWindow={ true }
                initialLoad={ false }>
                <ObjectsList dataType={ this.dataType.bind(this) }
                  selectPrefix={ this.selectPrefix.bind(this) }
                  showDeleteConfirmation={ this.showDeleteConfirmation.bind(this) }
                  shareObject={ this.shareObject.bind(this) }
                  checkObject={ this.checkObject.bind(this) }
                  checkedObjectsArray={ checkedObjects } />
              </InfiniteScroll>
              <div className="text-center" style={ { display: (istruncated && currentBucket) ? 'block' : 'none' } }>
                <span>Loading...</span>
              </div>
            </div>
            <UploadModal />
            { createButton }
            <Modal className="modal-create-bucket"
              bsSize="small"
              animation={ false }
              show={ showMakeBucketModal }
              onHide={ this.hideMakeBucketModal.bind(this) }>
              <button className="close close-alt" onClick={ this.hideMakeBucketModal.bind(this) }>
                <span>×</span>
              </button>
              <ModalBody>
                <form onSubmit={ this.makeBucket.bind(this) }>
                  <div className="input-group">
                    <input className="ig-text"
                      type="text"
                      ref="makeBucketRef"
                      placeholder="Bucket Name"
                      autoFocus/>
                    <i className="ig-helpers"></i>
                  </div>
                </form>
              </ModalBody>
            </Modal>
            <Modal className="modal-about modal-dark"
              animation={ false }
              show={ showAbout }
              onHide={ this.hideAbout.bind(this) }>
              <button className="close" onClick={ this.hideAbout.bind(this) }>
                <span>×</span>
              </button>
              <div className="ma-inner">
                <div className="mai-item hidden-xs">
                  <a href="https://minio.io" target="_blank"><img className="maii-logo" src={ logo } alt="" /></a>
                </div>
                <div className="mai-item">
                  <ul className="maii-list">
                    <li>
                      <div>
                        Version
                      </div>
                      <small>{ version }</small>
                    </li>
                    <li>
                      <div>
                        Memory
                      </div>
                      <small>{ memory }</small>
                    </li>
                    <li>
                      <div>
                        Platform
                      </div>
                      <small>{ platform }</small>
                    </li>
                    <li>
                      <div>
                        Runtime
                      </div>
                      <small>{ runtime }</small>
                    </li>
                  </ul>
                </div>
              </div>
            </Modal>
            <Modal className="modal-policy"
              animation={ false }
              show={ showBucketPolicy }
              onHide={ this.hideBucketPolicy.bind(this) }>
              <ModalHeader>
                Bucket Policy (
                { currentBucket })
                <button className="close close-alt" onClick={ this.hideBucketPolicy.bind(this) }>
                  <span>×</span>
                </button>
              </ModalHeader>
              <div className="pm-body">
                <PolicyInput bucket={ currentBucket } />
                { policies.map((policy, i) => <Policy key={ i } prefix={ policy.prefix } policy={ policy.policy } />
                  ) }
              </div>
            </Modal>
            <ConfirmModal show={ deleteConfirmation.show }
              icon='fa fa-exclamation-triangle mci-red'
              text='Are you sure you want to delete?'
              sub='This cannot be undone!'
              okText='Delete'
              cancelText='Cancel'
              okHandler={ this.removeObject.bind(this) }
              cancelHandler={ this.hideDeleteConfirmation.bind(this) }>
            </ConfirmModal>
            <Modal show={ shareObject.show }
              animation={ false }
              onHide={ this.hideShareObjectModal.bind(this) }
              bsSize="small">
              <ModalHeader>
                Share Object
              </ModalHeader>
              <ModalBody>
                <div className="input-group copy-text">
                  <label>
                    Shareable Link
                  </label>
                  <input type="text"
                    ref="copyTextInput"
                    readOnly="readOnly"
                    value={ window.location.protocol + '//' + shareObject.url }
                    onClick={ this.selectTexts.bind(this) } />
                </div>
                <div className="input-group" style={ { display: web.LoggedIn() ? 'block' : 'none' } }>
                  <label>
                    Expires in (Max 7 days)
                  </label>
                  <div className="set-expire">
                    <div className="set-expire-item">
                      <i className="set-expire-increase" onClick={ this.handleExpireValue.bind(this, 'expireDays', 1, shareObject.object) } />
                      <div className="set-expire-title">
                        Days
                      </div>
                      <div className="set-expire-value">
                        <input ref="expireDays"
                          type="number"
                          min={ 0 }
                          max={ 7 }
                          defaultValue={ 5 }
                          readOnly="readOnly"
                        />
                      </div>
                      <i className="set-expire-decrease" onClick={ this.handleExpireValue.bind(this, 'expireDays', -1, shareObject.object) } />
                    </div>
                    <div className="set-expire-item">
                      <i className="set-expire-increase" onClick={ this.handleExpireValue.bind(this, 'expireHours', 1, shareObject.object) } />
                      <div className="set-expire-title">
                        Hours
                      </div>
                      <div className="set-expire-value">
                        <input ref="expireHours"
                          type="number"
                          min={ 0 }
                          max={ 23 }
                          defaultValue={ 0 }
                          readOnly="readOnly"
                        />
                      </div>
                      <i className="set-expire-decrease" onClick={ this.handleExpireValue.bind(this, 'expireHours', -1, shareObject.object) } />
                    </div>
                    <div className="set-expire-item">
                      <i className="set-expire-increase" onClick={ this.handleExpireValue.bind(this, 'expireMins', 1, shareObject.object) } />
                      <div className="set-expire-title">
                        Minutes
                      </div>
                      <div className="set-expire-value">
                        <input ref="expireMins"
                          type="number"
                          min={ 0 }
                          max={ 59 }
                          defaultValue={ 0 }
                          readOnly="readOnly"
                        />
                      </div>
                      <i className="set-expire-decrease" onClick={ this.handleExpireValue.bind(this, 'expireMins', -1, shareObject.object) } />
                    </div>
                  </div>
                </div>
              </ModalBody>
              <div className="modal-footer">
                <CopyToClipboard text={ window.location.protocol + '//' + shareObject.url } onCopy={ this.showMessage.bind(this) }>
                  <button className="btn btn-success">
                    Copy Link
                  </button>
                </CopyToClipboard>
                <button className="btn btn-link" onClick={ this.hideShareObjectModal.bind(this) }>
                  Cancel
                </button>
              </div>
            </Modal>
            { settingsModal }
          </Dropzone>
        </div>
      </div>
    )
  }
}
