/*
 * Minio Cloud Storage (C) 2017 Minio, Inc.
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
import connect from 'react-redux/lib/components/connect'
import Thumbnail from './Thumbnail'
import * as actions from '../actions'

// Preview is the side panel for previewing info about objects.
class Preview extends React.Component {

  hidePreview(e) {
    const {dispatch} = this.props

    dispatch(actions.setPreviewStatus(false, '', ''))
    dispatch(actions.checkedObjectsReset())
  }

  render() {
    const {previewStatus} = this.props

    return (
      <aside className={ classNames({
                     'preview': true,
                     'preview--toggled': previewStatus.display
                   }) }>
        <div className="preview__header">
          <span className="preview__label">{ previewStatus.object }</span>
          <i className="preview__close zmdi zmdi-long-arrow-left" onClick={ this.hidePreview.bind(this) } />
        </div>
        <div className="preview__body">
          <div className="preview__item preview__item--img">
            <Thumbnail />
          </div>
          <div className="preview__info">
            <dl>
              <dt>Type</dt>
              <dd>
                JPEG Image
              </dd>
              <dt>Size</dt>
              <dd>
                1.1 MB
              </dd>
              <dt>Modified</dt>
              <dd>
                Mar 1, 2017 12:12 PM
              </dd>
              <dt>Policy</dt>
              <dd>
                Read and Write
              </dd>
              <dt>Shared</dt>
              <dd>
                No
              </dd>
            </dl>
          </div>
          <div className="preview__actions">
            <button className="btn btn--lg btn--primary">
              Download
            </button>
          </div>
        </div>
      </aside>
    )
  }
}

// Subscribe it to state changes.
export default connect(state => {
  return {
    previewStatus: state.previewStatus
  }
})(Preview)
