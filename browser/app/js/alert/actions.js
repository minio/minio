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

export const SET = "alert/SET"
export const CLEAR = "alert/CLEAR"

export let alertId = 0

export const set = alert => {
  const id = alertId++
  return (dispatch, getState) => {
    if (alert.type !== "danger" || alert.autoClear) {
      setTimeout(() => {
        dispatch({
          type: CLEAR,
          alert: {
            id
          }
        })
      }, 5000)
    }
    dispatch({
      type: SET,
      alert: Object.assign({}, alert, {
        id
      })
    })
  }
}

export const clear = () => {
  return { type: CLEAR }
}
