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

import React from "react"
import { connect } from "react-redux"
import { getCurrentBucket } from "../buckets/selectors"
import * as actionsObjects from "./actions"

export const Path = ({ currentBucket, currentPrefix, selectPrefix }) => {
  const onPrefixClick = (e, prefix) => {
    e.preventDefault()
    selectPrefix(prefix)
  }
  let dirPath = []
  let path = ""
  if (currentPrefix) {
    path = currentPrefix.split("/").map((dir, i) => {
      if (dir) {
        dirPath.push(dir)
        let dirPath_ = dirPath.join("/") + "/"
        return (
          <span key={i}>
            <a href="" onClick={e => onPrefixClick(e, dirPath_)}>
              {dir}
            </a>
          </span>
        )
      }
    })
  }

  return (
    <h2>
      <span className="main">
        <a onClick={e => onPrefixClick(e, "")} href="">
          {currentBucket}
        </a>
      </span>
      {path}
    </h2>
  )
}

const mapStateToProps = state => {
  return {
    currentBucket: getCurrentBucket(state),
    currentPrefix: state.objects.currentPrefix
  }
}

const mapDispatchToProps = dispatch => {
  return {
    selectPrefix: prefix => dispatch(actionsObjects.selectPrefix(prefix))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(Path)
