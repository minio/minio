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
import ObjectItem from "./ObjectItem"
import PrefixActions from "./PrefixActions"
import * as actionsObjects from "./actions"
import { getCheckedList } from "./selectors"

export const PrefixContainer = ({
  object,
  currentPrefix,
  checkedObjectsCount,
  selectPrefix
}) => {
  const props = {
    name: object.name,
    contentType: object.contentType,
    onClick: () => selectPrefix(`${currentPrefix}${object.name}`)
  }
  if (checkedObjectsCount == 0) {
    props.actionButtons = <PrefixActions object={object} />
  }
  return <ObjectItem {...props} />
}

const mapStateToProps = (state, ownProps) => {
  return {
    object: ownProps.object,
    currentPrefix: state.objects.currentPrefix,
    checkedObjectsCount: getCheckedList(state).length
  }
}

const mapDispatchToProps = dispatch => {
  return {
    selectPrefix: prefix => dispatch(actionsObjects.selectPrefix(prefix))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(PrefixContainer)
