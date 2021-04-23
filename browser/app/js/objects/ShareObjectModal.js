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
import { Modal, ModalHeader, ModalBody } from "react-bootstrap"
import CopyToClipboard from "react-copy-to-clipboard"
import web from "../web"
import * as objectsActions from "./actions"
import * as alertActions from "../alert/actions"
import {
  SHARE_OBJECT_EXPIRY_DAYS,
  SHARE_OBJECT_EXPIRY_HOURS,
  SHARE_OBJECT_EXPIRY_MINUTES
} from "../constants"
import QRCode from "react-qr-code";

export class ShareObjectModal extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      expiry: {
        days: SHARE_OBJECT_EXPIRY_DAYS,
        hours: SHARE_OBJECT_EXPIRY_HOURS,
        minutes: SHARE_OBJECT_EXPIRY_MINUTES
      }
    }
    this.expiryRange = {
      days: { min: 0, max: 7 },
      hours: { min: 0, max: 23 },
      minutes: { min: 0, max: 59 }
    }
  }
  updateExpireValue(param, inc) {
    let expiry = Object.assign({}, this.state.expiry)

    // Not allowing any increments if days is already max
    if (expiry.days == this.expiryRange["days"].max && inc > 0) {
      return
    }

    const { min, max } = this.expiryRange[param]
    expiry[param] = expiry[param] + inc
    if (expiry[param] < min || expiry[param] > max) {
      return
    }

    if (expiry.days == this.expiryRange["days"].max) {
      expiry.hours = 0
      expiry.minutes = 0
    } else if (expiry.days + expiry.hours + expiry.minutes == 0) {
      expiry.days = this.expiryRange["days"].max
    }

    this.setState({
      expiry
    })

    const { object, shareObject } = this.props
    shareObject(object.name, expiry.days, expiry.hours, expiry.minutes)
  }
  onUrlCopied() {
    const { showCopyAlert, hideShareObject } = this.props
    showCopyAlert("Link copied to clipboard!")
    hideShareObject()
  }
  render() {
    const { shareObjectDetails, hideShareObject } = this.props
    const url = `${window.location.protocol}//${shareObjectDetails.url}`
    return (
      <Modal
        show={true}
        animation={false}
        onHide={hideShareObject}
        bsSize="small"
      >
        <ModalHeader>Share Object</ModalHeader>
        <ModalBody>
          <div className="input-group copy-text">
            <QRCode value={url} size={128}/>
            <label>Shareable Link</label>
            <input
              type="text"
              ref={node => (this.copyTextInput = node)}
              readOnly="readOnly"
              value={url}
              onClick={() => this.copyTextInput.select()}
            />
          </div>
          {shareObjectDetails.showExpiryDate && (
            <div
            className="input-group"
            style={{ display: web.LoggedIn() ? "block" : "none" }}
          >
            <label>Expires in (Max 7 days)</label>
            <div className="set-expire">
              <div className="set-expire-item">
                <i
                  id="increase-days"
                  className="set-expire-increase"
                  onClick={() => this.updateExpireValue("days", 1)}
                />
                <div className="set-expire-title">Days</div>
                <div className="set-expire-value">
                  <input
                    ref="expireDays"
                    type="number"
                    min={0}
                    max={7}
                    value={this.state.expiry.days}
                    readOnly="readOnly"
                  />
                </div>
                <i
                  id="decrease-days"
                  className="set-expire-decrease"
                  onClick={() => this.updateExpireValue("days", -1)}
                />
              </div>
              <div className="set-expire-item">
                <i
                  id="increase-hours"
                  className="set-expire-increase"
                  onClick={() => this.updateExpireValue("hours", 1)}
                />
                <div className="set-expire-title">Hours</div>
                <div className="set-expire-value">
                  <input
                    ref="expireHours"
                    type="number"
                    min={0}
                    max={23}
                    value={this.state.expiry.hours}
                    readOnly="readOnly"
                  />
                </div>
                <i
                  className="set-expire-decrease"
                  id="decrease-hours"
                  onClick={() => this.updateExpireValue("hours", -1)}
                />
              </div>
              <div className="set-expire-item">
                <i
                  id="increase-minutes"
                  className="set-expire-increase"
                  onClick={() => this.updateExpireValue("minutes", 1)}
                />
                <div className="set-expire-title">Minutes</div>
                <div className="set-expire-value">
                  <input
                    ref="expireMins"
                    type="number"
                    min={0}
                    max={59}
                    value={this.state.expiry.minutes}
                    readOnly="readOnly"
                  />
                </div>
                <i
                  id="decrease-minutes"
                  className="set-expire-decrease"
                  onClick={() => this.updateExpireValue("minutes", -1)}
                />
              </div>
            </div>
          </div>
          )}
        </ModalBody>
        <div className="modal-footer">
          <CopyToClipboard
            text={url}
            onCopy={this.onUrlCopied.bind(this)}
          >
            <button className="btn btn-success">Copy Link</button>
          </CopyToClipboard>
          <button className="btn btn-link" onClick={hideShareObject}>
            Cancel
          </button>
        </div>
      </Modal>
    )
  }
}

const mapStateToProps = (state, ownProps) => {
  return {
    object: ownProps.object,
    shareObjectDetails: state.objects.shareObject
  }
}

const mapDispatchToProps = dispatch => {
  return {
    shareObject: (object, days, hours, minutes) =>
      dispatch(objectsActions.shareObject(object, days, hours, minutes)),
    hideShareObject: () => dispatch(objectsActions.hideShareObject()),
    showCopyAlert: message =>
      dispatch(alertActions.set({ type: "success", message: message }))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(ShareObjectModal)
