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

import "babel-polyfill"
import "./less/main.less"
import "@fortawesome/fontawesome-free/css/all.css"
import "material-design-iconic-font/dist/css/material-design-iconic-font.min.css"

import React from "react"
import ReactDOM from "react-dom"
import { Router, Route } from "react-router-dom"
import { Provider } from "react-redux"

import history from "./js/history"
import configureStore from "./js/store/configure-store"
import hideLoader from "./js/loader"
import App from "./js/App"

const store = configureStore()

ReactDOM.render(
  <Provider store={store}>
    <Router history={history}>
      <App />
    </Router>
  </Provider>,
  document.getElementById("root")
)

hideLoader()
