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
import classNames from "classnames"
import { CSSTransition } from "react-transition-group"

export class Alert extends React.Component {
  render() {
    const { show, type, message, onDismiss } = this.props
    const AlertProps = {
      unmountOnExit: true,
      timeout: {
        enter: 10,
        exit: 300
      },
      classNames: {
        enter: "alert--enter",
        enterDone: "alert--enter-done",
        exit: "alert--exit",
        exitDone: "alert--exit-done"
      }
    }

    return (
      <CSSTransition in={show} {...AlertProps}>
        {state => (
          <div
            className={classNames({
              alert: true,
              [`alert--${type}`]: true
            })}
          >
            <div className="alert__message">{message}</div>
            <i className="alert__close" onClick={onDismiss} />
          </div>
        )}
      </CSSTransition>
    )
  }
}

export default Alert
