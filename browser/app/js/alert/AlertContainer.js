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
import Alert from "./Alert"
import * as alertActions from "./actions"

export const AlertContainer = ({ alert, clearAlert }) => {
  if (!alert.message) {
    return ""
  }
  return <Alert {...alert} onDismiss={clearAlert} />
}

const mapStateToProps = state => {
  return {
    alert: state.alert
  }
}

const mapDispatchToProps = dispatch => {
  return {
    clearAlert: () => dispatch(alertActions.clear())
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(AlertContainer)
