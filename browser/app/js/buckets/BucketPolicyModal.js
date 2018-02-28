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
import { Modal, ModalHeader } from "react-bootstrap"
import * as actionsBuckets from "./actions"
import PolicyInput from "./PolicyInput"
import Policy from "./Policy"

export const BucketPolicyModal = ({ showBucketPolicy, currentBucket, hideBucketPolicy, policies }) => {
  return (
    <Modal className="modal-policy"
            animation={ false }
            show={ showBucketPolicy }
            onHide={ hideBucketPolicy }
    >
      <ModalHeader>
        Bucket Policy (
        { currentBucket })
        <button className="close close-alt" onClick={ hideBucketPolicy }>
          <span>×</span>
        </button>
      </ModalHeader>
      <div className="pm-body">
        <PolicyInput />
        { policies.map((policy, i) => <Policy key={ i } prefix={ policy.prefix } policy={ policy.policy } />
          ) }
      </div>
    </Modal>
  )
}

const mapStateToProps = state => {
  return {
    currentBucket: state.buckets.currentBucket,
    showBucketPolicy: state.buckets.showBucketPolicy,
    policies: state.buckets.policies
  }
}

const mapDispatchToProps = dispatch => {
  return {
    hideBucketPolicy: () => dispatch(actionsBuckets.hideBucketPolicy())
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(BucketPolicyModal)