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

import * as actionsAlert from "./actions"

const initialState = {
  show: false,
  type: "danger"
}
export default (state = initialState, action) => {
  switch (action.type) {
    case actionsAlert.SET:
      return {
        show: true,
        id: action.alert.id,
        type: action.alert.type,
        message: action.alert.message
      }
    case actionsAlert.CLEAR:
      if (action.alert && action.alert.id != state.id) {
        return state
      } else {
        return initialState
      }
    default:
      return state
  }
}
