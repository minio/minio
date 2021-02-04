/*
 * MinIO Cloud Storage (C) 2018 MinIO, Inc.
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
