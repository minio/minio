/*
 * Minio Cloud Storage (C) 2016 Minio, Inc.
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
import 'babel-polyfill'

import './less/main.less'

import React from 'react'
import ReactDOM from 'react-dom'
import thunkMiddleware from 'redux-thunk'
import createStore from 'redux/lib/createStore'
import applyMiddleware from 'redux/lib/applyMiddleware'

import Route from 'react-router/lib/Route'
import Router from 'react-router/lib/Router'
import browserHistory from 'react-router/lib/browserHistory'
import IndexRoute from 'react-router/lib/IndexRoute'

import Provider from 'react-redux/lib/components/Provider'
import connect from 'react-redux/lib/components/connect'

import Moment from 'moment'

import { minioBrowserPrefix } from './js/constants.js'
import * as actions from './js/actions.js'
import reducer from './js/reducers.js'

import _Login from './js/components/Login.js'
import _Browse from './js/components/Browse.js'
import fontAwesome from 'font-awesome/css/font-awesome.css'

import MaterialDesignIconicFonts from 'material-design-iconic-font/dist/css/material-design-iconic-font.min.css'

import Web from './js/web'
window.Web = Web

import storage from 'local-storage-fallback'
const store = applyMiddleware(thunkMiddleware)(createStore)(reducer)
const Browse = connect(state => state)(_Browse)
const Login = connect(state => state)(_Login)

let web = new Web(`${window.location.protocol}//${window.location.host}${minioBrowserPrefix}/webrpc`, store.dispatch)

window.web = web

store.dispatch(actions.setWeb(web))

function authNeeded(nextState, replace, cb) {
  if (web.LoggedIn()) {
    return cb()
  }
  if (location.pathname === minioBrowserPrefix || location.pathname === minioBrowserPrefix + '/') {
    replace(`${minioBrowserPrefix}/login`)
  }
  return cb()
}

function authNotNeeded(nextState, replace) {
  if (web.LoggedIn()) {
    replace(`${minioBrowserPrefix}`)
  }
}

const App = (props) => {
  return <div>
           { props.children }
         </div>
}

ReactDOM.render((
  <Provider store={ store } web={ web }>
    <Router history={ browserHistory }>
      <Route path='/' component={ App }>
        <Route path={ minioBrowserPrefix } component={ App }>
          <IndexRoute component={ Browse } onEnter={ authNeeded } />
          <Route path='login' component={ Login } onEnter={ authNotNeeded } />
          <Route path=':bucket' component={ Browse } onEnter={ authNeeded } />
          <Route path=':bucket/*' component={ Browse } onEnter={ authNeeded } />
        </Route>
      </Route>
    </Router>
  </Provider>
  ), document.getElementById('root'))

//Page loader
let delay = [0, 400]
let i = 0

function handleLoader() {
  if (i < 2) {
    setTimeout(function() {
      document.querySelector('.page-load').classList.add('pl-' + i)
      i++
      handleLoader()
    }, delay[i])
  }
}
handleLoader()

if (storage.getItem('newlyUpdated')) {
  store.dispatch(actions.showAlert({
    type: 'success',
    message: "Updated to the latest UI Version."
  }))
  storage.removeItem('newlyUpdated')
}
