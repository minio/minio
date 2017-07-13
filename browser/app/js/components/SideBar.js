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
import logo from '../../img/logo-dark.svg'
import Dropdown from 'react-bootstrap/lib/Dropdown'

let SideBar = ({visibleBuckets, loadBucket, currentBucket, selectBucket, searchBuckets, sidebarStatus, clickOutside, showPolicy, deleteBucket, storageDetails}) => {

  const list = visibleBuckets.map((bucket, i) => {
    return <li className={ classNames({
                  'buckets__list__active': bucket === currentBucket,
                  'fesli-loading': bucket === loadBucket
                }) } key={ i } onClick={ (e) => selectBucket(e, bucket) }>
             <div className="buckets__list__name">
               { bucket }
             </div>
             <div className="buckets__list__policy">
               read and write
             </div>
             <Dropdown pullRight id="dropdown-bucket-actions" className={ 'buckets__list__actions' }>
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
    <aside className={ classNames({
                     'sidebar': true,
                     'sidebar--toggled': sidebarStatus
                   }) }>
      <div className="logo">
        <img className="logo__img" src={ logo } alt="" />
        <div className="logo__title">
          <h2>Minio Browser</h2>
          <small>{ window.location.host }</small>
        </div>
      </div>
      <div className="buckets">
        <div className="buckets__search" style={ { display: web.LoggedIn() ? 'block' : 'none' } }>
          <div className="form-group">
            <input className="form-group__field"
              type="text"
              onChange={ searchBuckets }
              placeholder="Search Buckets..." />
            <i className="form-group__bar"></i>
          </div>
        </div>
        <ul className="buckets__list">
          { list }
        </ul>
      </div>
      { storageDetails }
    </aside>
  )
}

// Subscribe it to state changes that affect only the sidebar.
export default connect(state => {
  return {
    visibleBuckets: state.visibleBuckets,
    loadBucket: state.loadBucket,
    currentBucket: state.currentBucket,
    sidebarStatus: state.sidebarStatus
  }
})(SideBar)
