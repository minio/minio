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