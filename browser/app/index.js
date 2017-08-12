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

import Vue from 'vue'
import VueRouter from 'vue-router'

Vue.use(VueRouter)

// This is our store, a global way of managing state in the application.
import { store } from './js/store'

// This allows us to manipulate the server.
import Web from './js/web'
store.state.web = new Web(`${window.location.protocol}//${window.location.host}${minioBrowserPrefix}/webrpc`, vm)

// Initialize our routes.
import { minioBrowserPrefix } from './js/constants'
import { requireAuth } from './js/auth'

import Login from './js/components/Login.vue'
import Browse from './js/components/Browse.vue'

const router = new VueRouter({
  // This allows us to have host/minio/xxx, instead of host/#/minio/xxx.
  mode: 'history',

  routes: [{
    beforeEnter: requireAuth,
    path: minioBrowserPrefix,
    component: Browse
  }, {
    beforeEnter: requireAuth,
    path: minioBrowserPrefix + '/bucket/:bucket',
    component: Browse
  }, {
    /* TODO: show login on unauthenticated /, not as own route,
       change above route to /:bucket, see harsha */
    path: minioBrowserPrefix + '/login',
    component: Login
  }, {
    // Redirect / to /minio.
    path: '/',
    redirect: minioBrowserPrefix
  }, {
    path: '*',
    component: {
      /* TODO create a not found error */
      template: '<h1>404 not found, ha</h1>'
    }
  }]
})

const vm = new Vue({
  el: document.getElementById('root'),

  // Inject the router.
  router,

  // Inject the store.
  store
})

// This gives us a nice and pretty page loader.
// First fade it out, then hide it entirely.
setTimeout(() => {
  document.querySelector('.page-load').classList.add('pl-0')
}, 50)

setTimeout(() => {
  document.querySelector('.page-load').classList.add('pl-1')
}, 400)
