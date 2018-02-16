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
import MobileHeader from "./MobileHeader"
import Header from "./Header"
import ObjectsSection from "../objects/ObjectsSection"
import MainActions from "./MainActions"
import MakeBucketModal from "../buckets/MakeBucketModal"
import UploadModal from "../uploads/UploadModal"
import ObjectsBulkActions from "../objects/ObjectsBulkActions"

export const MainContent = () => (
  <div className="fe-body">
    <ObjectsBulkActions />
    <MobileHeader />
    <Header />
    <ObjectsSection />
    <MainActions />
    <MakeBucketModal />
    <UploadModal />
  </div>
)

export default MainContent
