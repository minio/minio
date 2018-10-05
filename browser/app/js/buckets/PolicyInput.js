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
import * as actionsAlert from "../alert/actions"
import web from "../web"
import ReactTooltip from "react-tooltip"
import { READ_ONLY, WRITE_ONLY, READ_WRITE } from "../constants"

export class PolicyInput extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      permissionSelected: READ_ONLY
    }
  }

  handlePermission(e) {
    this.setState({
      permissionSelected: e.target.value
    })
  }

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

    if (this.prefix.value === "*") this.prefix.value = ""

    let policyAlreadyExists = this.props.policies.some(
      elem =>
        this.prefix.value === elem.prefix && this.policy.value === elem.policy
    )
    if (policyAlreadyExists) {
      showAlert("danger", "Policy for this prefix already exists.")
      return
    }

    web
      .SetBucketPolicy({
        bucketName: currentBucket,
        prefix: this.prefix.value,
        policy: this.state.permissionSelected
      })
      .then(() => {
        fetchPolicies(currentBucket)
        this.prefix.value = ""
      })
      .catch(e => showAlert("danger", e.message))
  }

  render() {
    return (
      <div className="policy__header">
        <div className="policy__prefix">
          <div className="policy__label">Prefix</div>
          <div className="form-item">
            <input
              type="text"
              className="form-item__input"
              ref={prefix => (this.prefix = prefix)}
            />
          </div>
        </div>

        <div className="policy__permission">
          <div className="policy__label">Permission</div>

          <div className="select-group">
            <div
              className="select-group__item"
              data-tip="Read Only"
              data-for="tooltip-permission"
            >
              <input
                type="radio"
                name="policy-permission"
                defaultChecked
                value={READ_ONLY}
                ref={policy => (this.policy = policy)}
                onChange={this.handlePermission.bind(this)}
              />
              <i>R</i>
            </div>

            <div
              className="select-group__item"
              data-tip="Write Only"
              data-for="tooltip-permission"
            >
              <input
                type="radio"
                name="policy-permission"
                value={WRITE_ONLY}
                ref={policy => (this.policy = policy)}
                onChange={this.handlePermission.bind(this)}
              />
              <i>W</i>
            </div>

            <div
              className="select-group__item"
              data-tip="Read and Write"
              data-for="tooltip-permission"
            >
              <input
                type="radio"
                name="policy-permission"
                value={READ_WRITE}
                ref={policy => (this.policy = policy)}
                onChange={this.handlePermission.bind(this)}
              />
              <i>RW</i>
            </div>
          </div>

          <ReactTooltip
            id="tooltip-permission"
            effect="solid"
            place="bottom"
            className="tooltip"
          />
        </div>

        <button
          className="button button--success"
          onClick={this.handlePolicySubmit.bind(this)}
        >
          Add
        </button>
      </div>
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
