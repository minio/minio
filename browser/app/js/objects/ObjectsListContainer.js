/*
 * MinIO Cloud Storage (C) 2018 MinIO, Inc.
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
import InfiniteScroll from "react-infinite-scroller"
import ObjectsList from "./ObjectsList"

export class ObjectsListContainer extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      page: 1
    }
    this.loadNextPage = this.loadNextPage.bind(this)
  }
  componentWillReceiveProps(nextProps) {
    if (
      nextProps.currentBucket !== this.props.currentBucket ||
      nextProps.currentPrefix !== this.props.currentPrefix ||
      nextProps.sortBy !== this.props.sortBy ||
      nextProps.sortOrder !== this.props.sortOrder
    ) {
      this.setState({
        page: 1
      })
    }
  }
  loadNextPage() {
    this.setState(state => {
      return { page: state.page + 1 }
    })
  }
  render() {
    const { objects, listLoading } = this.props

    const visibleObjects = objects.slice(0, this.state.page * 100)

    return (
      <div style={{ position: "relative" }}>
        <InfiniteScroll
          pageStart={0}
          loadMore={this.loadNextPage}
          hasMore={objects.length > visibleObjects.length}
          useWindow={true}
          initialLoad={false}
        >
          <ObjectsList objects={visibleObjects} />
        </InfiniteScroll>
        {listLoading && <div className="loading" />}
      </div>
    )
  }
}

const mapStateToProps = state => {
  return {
    currentBucket: state.buckets.currentBucket,
    currentPrefix: state.objects.currentPrefix,
    objects: state.objects.list,
    sortBy: state.objects.sortBy,
    sortOrder: state.objects.sortOrder,
    listLoading: state.objects.listLoading
  }
}

export default connect(mapStateToProps)(ObjectsListContainer)
