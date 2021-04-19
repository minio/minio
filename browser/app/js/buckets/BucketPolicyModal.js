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