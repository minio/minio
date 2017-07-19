import { READ_ONLY, WRITE_ONLY, READ_WRITE } from '../constants'
import React, { Component, PropTypes } from 'react'
import connect from 'react-redux/lib/components/connect'
import classnames from 'classnames'
import * as actions from '../actions'

class PolicyInput extends Component {
  componentDidMount() {
    const {web, dispatch} = this.props
    web.ListAllBucketPolicies({
      bucketName: this.props.currentBucket
    }).then(res => {
      let policies = res.policies
      if (policies) dispatch(actions.setPolicies(policies))
    }).catch(err => {
      dispatch(actions.showAlert({
        type: 'danger',
        message: err.message
      }))
    })
  }

  componentWillUnmount() {
    const {dispatch} = this.props
    dispatch(actions.setPolicies([]))
  }

  handlePolicySubmit(e) {
    e.preventDefault()
    const {web, dispatch} = this.props

    web.SetBucketPolicy({
      bucketName: this.props.currentBucket,
      prefix: this.prefix.value,
      policy: this.policy.value
    })
      .then(() => {
        dispatch(actions.setPolicies([{
          policy: this.policy.value,
          prefix: this.prefix.value + '*',
        }, ...this.props.policies]))
        this.prefix.value = ''
      })
      .catch(e => dispatch(actions.showAlert({
        type: 'danger',
        message: e.message,
      })))
  }

  render() {
    return (
      <header className="policy__list">
        <div className="policy__item">
          <div className="form-group">
            <input type="text"
              ref={ prefix => this.prefix = prefix }
              className="form-group__field form-group__field--sm"
              placeholder="Prefix"
              editable={ true } />
            <i className="form-group__bar" />
          </div>
        </div>
        <div className="policy__item">
          <select ref={ policy => this.policy = policy } className="form-group__field form-group__field--sm">
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
        <div className="policy__item">
          <button className="btn btn--block btn--primary" onClick={ this.handlePolicySubmit.bind(this) }>
            Add
          </button>
        </div>
      </header>
    )
  }
}

export default connect(state => state)(PolicyInput)