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
import ClickOutHandler from 'react-onclickout'
import Scrollbars from 'react-custom-scrollbars/lib/Scrollbars'
import connect from 'react-redux/lib/components/connect'

import logo from '../../img/logo.svg'
import Dropdown from 'react-bootstrap/lib/Dropdown'

let SideBar = ({visibleBuckets, loadBucket, currentBucket, selectBucket, searchBuckets, sidebarStatus, clickOutside, showPolicy, deleteBucket, toggleBucketDropdown, showBucketDropdown}) => {

  const list = visibleBuckets.map((bucket, i) => {
    return <li className={ classNames({
                  'active': bucket === currentBucket
                }) } key={ i } onClick={ (e) => selectBucket(e, bucket) }>
             <a href="" className={ classNames({
                                      'fesli-loading': bucket === loadBucket
                                    }) }>
               { bucket }
             </a>
             <Dropdown open={bucket === currentBucket && showBucketDropdown} onToggle={toggleBucketDropdown} className="bucket-dropdown" id="bucket-dropdown">
               <Dropdown.Toggle noCaret>
                 <i className="zmdi zmdi-more-vert" />
               </Dropdown.Toggle>
               <Dropdown.Menu className="dropdown-menu-right">
                 <li>
                   <a onClick={ showPolicy }>Edit policy</a>
                 </li>
                 <li>
                   <a onClick={ (e) => deleteBucket(e, bucket) }>Delete</a>
                 </li>
               </Dropdown.Menu>
             </Dropdown>
           </li>
  })

  return (
    <ClickOutHandler onClickOut={ clickOutside }>
      <div className={ classNames({
                         'fe-sidebar': true,
                         'toggled': sidebarStatus
                       }) }>
        <div className="fes-header clearfix hidden-sm hidden-xs">
          <img src={ logo } alt="" />
          <h2>Minio Browser</h2>
        </div>
        <div className="fes-list">
          <div className="input-group ig-dark ig-left ig-search" style={ { display: web.LoggedIn() ? 'block' : 'none' } }>
            <input className="ig-text"
              type="text"
              onChange={ searchBuckets }
              placeholder="Search Buckets..." />
            <i className="ig-helpers"></i>
          </div>
          <div className="fesl-inner">
            <Scrollbars renderScrollbarVertical={ props => <div className="scrollbar-vertical" /> }>
              <ul>
                { list }
              </ul>
            </Scrollbars>
          </div>
        </div>
        <div className="fes-host">
          <i className="fa fa-globe"></i>
          <a href="/">
            { window.location.host }
          </a>
        </div>
      </div>
    </ClickOutHandler>
  )
}

// Subscribe it to state changes that affect only the sidebar.
export default connect(state => {
  return {
    visibleBuckets: state.visibleBuckets,
    loadBucket: state.loadBucket,
    currentBucket: state.currentBucket,
    showBucketDropdown: state.showBucketDropdown,
    sidebarStatus: state.sidebarStatus
  }
})(SideBar)
