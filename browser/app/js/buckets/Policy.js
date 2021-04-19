/*
 * Copyright (c) 2015-2021 MinIO, Inc.
 *
 * This file is part of MinIO Object Storage stack
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import { READ_ONLY, WRITE_ONLY, READ_WRITE, NONE } from '../constants'

import React from "react"
import { connect } from "react-redux"
import classnames from "classnames"
import * as actionsBuckets from "./actions"
import * as actionsAlert from "../alert/actions"
import web from "../web"

export class Policy extends React.Component {
  removePolicy(e) {
    e.preventDefault()
    const {currentBucket, prefix, fetchPolicies, showAlert} = this.props
    web.
      SetBucketPolicy({
        bucketName: currentBucket,
        prefix: prefix,
        policy: 'none'
      })
      .then(() => {
        fetchPolicies(currentBucket)
      })
      .catch(e => showAlert('danger', e.message))
  }

  render() {
    const {policy, prefix} = this.props
    let newPrefix = prefix
    if (newPrefix === '')
      newPrefix = '*'

    if (policy === NONE) {
      return <noscript />
    } else {
      return (
        <div className="pmb-list">
          <div className="pmbl-item">
            { newPrefix }
          </div>
          <div className="pmbl-item">
            <select className="form-control"
              disabled
              value={ policy }>
              <option value={ READ_ONLY }>
                Read Only
              </option>
              <option value={ WRITE_ONLY }>
                Write Only
              </option>
              <option value={ READ_WRITE }>
                Read and Write
              </option>
            </select>
          </div>
          <div className="pmbl-item">
            <button className="btn btn-block btn-danger" onClick={ this.removePolicy.bind(this) }>
              Remove
            </button>
          </div>
        </div>
      )
    }
  }
}

const mapStateToProps = state => {
  return {
    currentBucket: state.buckets.currentBucket
  }
}

const mapDispatchToProps = dispatch => {
  return {
    fetchPolicies: bucket => dispatch(actionsBuckets.fetchPolicies(bucket)),
    showAlert: (type, message) =>
      dispatch(actionsAlert.set({ type: type, message: message }))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(Policy)