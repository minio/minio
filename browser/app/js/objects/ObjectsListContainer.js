/*
 * Minio Cloud Storage (C) 2018 Minio, Inc.
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
import classNames from "classnames"
import { connect } from "react-redux"
import InfiniteScroll from "react-infinite-scroller"
import * as actionsObjects from "./actions"
import ObjectsList from "./ObjectsList"

export class ObjectsListContainer extends React.Component {
  render() {
    const { objects, isTruncated, currentBucket, loadObjects } = this.props
    return (
      <div className="feb-container">
        <InfiniteScroll
          pageStart={0}
          loadMore={() => loadObjects(true)}
          hasMore={isTruncated}
          useWindow={true}
          initialLoad={false}
        >
          <ObjectsList objects={objects} />
        </InfiniteScroll>
        <div
          className="text-center"
          style={{ display: isTruncated && currentBucket ? "block" : "none" }}
        >
          <span>Loading...</span>
        </div>
      </div>
    )
  }
}

const mapStateToProps = state => {
  return {
    currentBucket: state.buckets.currentBucket,
    currentPrefix: state.objects.currentPrefix,
    objects: state.objects.list,
    isTruncated: state.objects.isTruncated
  }
}

const mapDispatchToProps = dispatch => {
  return {
    loadObjects: append => dispatch(actionsObjects.fetchObjects(append))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(
  ObjectsListContainer
)
