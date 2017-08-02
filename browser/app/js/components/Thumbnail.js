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
import connect from 'react-redux/lib/components/connect'
import storage from 'local-storage-fallback'
import mime from 'mime-types'
import * as actions from '../actions'

// Thumbnail is an asynchronously loaded thumbnail image.
class Thumbnail extends React.Component {

  constructor(props) {
    super(props);

    // The url for the thumbnail <img>
    this.state = {
      xhr: null,
      url: ''
    }
  }

  componentWillUnmount() {
    this.cleanup()
  }

  componentWillReceiveProps(nextProps) {
    const oldPreviewStatus = this.props.previewStatus
    const {previewStatus} = nextProps

    // If it hasn't changed, don't update it.
    if (oldPreviewStatus.bucket == previewStatus.bucket &&
      oldPreviewStatus.object == previewStatus.object) {
      return
    }

    // It has changed, so we should cleanup what came before.
    // This prevents any image lag.
    this.cleanup()

    // If we're hiding the panel, don't load anything else.
    if (previewStatus.bucket == '' && previewStatus.object == '') {
      return
    }

    // Not all objects can be previewed. Make sure we can before continuing.
    if (!this.canPreview(previewStatus.object)) {
      return
    }

    // Create a new XHR for loading the image.
    const url = `${window.location.origin}/minio/thumbnail/${previewStatus.bucket}/${previewStatus.object}`

    let xhr = new XMLHttpRequest()
    xhr.open('GET', url, true)
    xhr.responseType = 'blob'

    xhr.setRequestHeader("Authorization", 'Bearer ' + storage.getItem('token'))
    xhr.send()

    xhr.onload = (response) => {
      if (xhr.status != 200) {
        // Oops. This might mean the image isn't formatted correctly, but
        // has an image file extension.
        this.setState({
          url: '#'
        })
        return
      }

      const imageUrl = window.URL.createObjectURL(xhr.response)

      this.setState({
        url: imageUrl
      })
    }
  }

  cleanup() {
    if (this.state.xhr) {
      this.state.xhr.abort()
    }

    this.setState({
      xhr: null,
      url: ''
    })
  }

  canPreview(filename) {
    // Returns a boolean, whether or not we can preview this file.
    const acceptable = ['image/jpeg', 'image/png', 'image/gif']
    const type = mime.lookup(filename)

    return (acceptable.indexOf(type) != -1)
  }

  render() {
    const {url} = this.state

    // If no URL is present, don't render anything yet.
    if (url == '')
      return (null)

    // If we do have a URL, display it!
    return (
      <img src={ url } alt="Cannot display thumbnail (not an image)." />
    )
  }
}

// Subscribe it to state changes.
export default connect(state => {
  return {
    previewStatus: state.previewStatus
  }
})(Thumbnail)
