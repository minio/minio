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

import { READ_ONLY, WRITE_ONLY, READ_WRITE } from '../constants'

import React from "react"
import { connect } from "react-redux"
import classnames from "classnames"
import * as actionsBuckets from "./actions"
import * as actionsAlert from "../alert/actions"
import web from "../web"

export class PolicyInput extends React.Component {
  componentDidMount() {
    const { currentBucket, fetchPolicies } = this.props
    fetchPolicies(currentBucket)
  }

  componentWillUnmount() {
    const { setPolicies } = this.props
    setPolicies([])
  }

  handlePolicySubmit(e) {
    e.preventDefault()
    const { currentBucket, fetchPolicies, showAlert } = this.props
    
    if (this.prefix.value === "*")
      this.prefix.value = ""
    
    let policyAlreadyExists = this.props.policies.some(
      elem => this.prefix.value === elem.prefix && this.policy.value === elem.policy
    )
    if (policyAlreadyExists) {
      showAlert("danger", "Policy for this prefix already exists.")
      return
    }
    
    web.
      SetBucketPolicy({
        bucketName: currentBucket,
        prefix: this.prefix.value,
        policy: this.policy.value
      })
      .then(() => {
        fetchPolicies(currentBucket)
        this.prefix.value = ''
      })
      .catch(e => showAlert("danger", e.message))
  }

  render() {
    return (
      <header className="pmb-list">
        <div className="pmbl-item">
          <input 
            type="text"
            ref={ prefix => this.prefix = prefix }
            className="form-control"
            placeholder="Prefix"
          />
        </div>
        <div className="pmbl-item">
          <select ref={ policy => this.policy = policy } className="form-control">
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
          <button className="btn btn-block btn-primary" onClick={ this.handlePolicySubmit.bind(this) }>
            Add
          </button>
        </div>
      </header>
    )
  }
}

const mapStateToProps = state => {
  return {
    currentBucket: state.buckets.currentBucket,
    policies: state.buckets.policies
  }
}

const mapDispatchToProps = dispatch => {
  return {
    fetchPolicies: bucket => dispatch(actionsBuckets.fetchPolicies(bucket)),
    setPolicies: policies => dispatch(actionsBuckets.setPolicies(policies)),
    showAlert: (type, message) =>
      dispatch(actionsAlert.set({ type: type, message: message }))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(PolicyInput)