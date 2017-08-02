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
import Moment from 'moment'
import humanize from 'humanize'
import connect from 'react-redux/lib/components/connect'
import Dropdown from 'react-bootstrap/lib/Dropdown'
import MaterialDesignIconicFonts from 'material-design-iconic-font/dist/css/material-design-iconic-font.min.css'

let ObjectsList = ({objects, currentPath, selectPrefix, dataType, showDeleteConfirmation, shareObject, loadPath, checkObject, checkedObjectsArray, currentBucket, showObjectPreview}) => {
  const list = objects.map((object, i) => {
    let size = object.name.endsWith('/') ? '' : humanize.filesize(object.size)
    let lastModified = object.name.endsWith('/') ? '' : Moment(object.lastModified).format('lll')
    let loadingClass = loadPath === `${currentPath}${object.name}` ? 'fesl-loading' : ''

    let activeClass = ''
    let isChecked = ''
    let folderClass = dataType(object.name, object.contentType) == 'folder' ? 'objects__row--folder' : ''


    if (checkedObjectsArray.indexOf(object.name) > -1) {
      activeClass = ' objects__row-selected'
      isChecked = true
    }


    return (
      <div key={ i } className={ "objects__row " + folderClass + loadingClass + activeClass } onClick={ (e) => showObjectPreview(e, currentBucket, object.name) }>
        <div className="objects__item objects__item--select" data-object-type={ dataType(object.name, object.contentType) }>
          <div className="checkbox">
            <input type="checkbox"
              name={ object.name }
              checked={ isChecked }
              onChange={ (e) => checkObject(e, object.name) } />
            <i className="checkbox__helper" />
          </div>
        </div>
        <div className="objects__item objects__item--name">
          <a href="" onClick={ (e) => selectPrefix(e, `${currentPath}${object.name}`) }>
            { object.name }
          </a>
        </div>
        <div className="objects__item objects__item--size">
          { size }
        </div>
        <div className="objects__item objects__item--modified">
          { lastModified }
        </div>
      </div>
    )
  })
  return (
    <div>
      { list }
    </div>
  )
}

// Subscribe it to state changes.
export default connect(state => {
  return {
    objects: state.objects,
    currentPath: state.currentPath,
    loadPath: state.loadPath
  }
})(ObjectsList)