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
import { connect } from "react-redux"
import * as actionsBuckets from "./actions"

export class BucketSearch extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      searchValue: ""
    }
  }

  clearSearch(e) {
    if (e.keyCode === 27) {
      const { onChange } = this.props
      onChange("")
      this.setState({
        searchValue: ""
      })
    }
  }

  searchOnChange(e) {
    const { onChange } = this.props
    this.setState({
      searchValue: e.target.value
    })
    onChange(e.target.value)
  }

  render() {
    return (
      <div
        className="bucket-search"
        tabIndex="0"
        onKeyDown={this.clearSearch.bind(this)}
      >
        <input
          className="bucket-search__text"
          type="text"
          onChange={this.searchOnChange.bind(this)}
          placeholder="Search Buckets..."
          value={this.state.searchValue}
        />
      </div>
    )
  }
}

const mapDispatchToProps = dispatch => {
  return {
    onChange: filter => {
      dispatch(actionsBuckets.setFilter(filter))
    }
  }
}

export default connect(undefined, mapDispatchToProps)(BucketSearch)
