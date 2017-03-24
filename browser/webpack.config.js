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

var webpack = require('webpack')
var path = require('path')
var CopyWebpackPlugin = require('copy-webpack-plugin')
var purify = require("purifycss-webpack-plugin")

var exports = {
  context: __dirname,
  entry: [
    path.resolve(__dirname, 'app/index.js')
  ],
  output: {
    path: path.resolve(__dirname, 'dev'),
    filename: 'index_bundle.js',
    publicPath: '/minio/'
  },
  module: {
    loaders: [{
        test: /\.js$/,
        exclude: /(node_modules|bower_components)/,
        loader: 'babel',
        query: {
              presets: ['react', 'es2015']
            }
      }, {
        test: /\.less$/,
        loader: 'style!css!less'
      }, {
        test: /\.json$/,
        loader: 'json-loader'
      },{
        test: /\.css$/,
        loader: 'style!css'
      }, {
        test: /\.(eot|woff|woff2|ttf|svg|png)/,
        loader: 'url'
      }]
  },
  node:{
    fs:'empty'
  },
  devServer: {
    historyApiFallback: {
      index: '/minio/'
    },
    proxy: {
      '/minio/webrpc': {
	target: 'http://localhost:9000',
	secure: false
      },
      '/minio/upload/*': {
	target: 'http://localhost:9000',
	secure: false
      },
      '/minio/download/*': {
	target: 'http://localhost:9000',
	secure: false
      },
      '/minio/zip': {
        target: 'http://localhost:9000',
        secure: false
      }
    }
  },
  plugins: [
    new CopyWebpackPlugin([
      {from: 'app/css/loader.css'},
      {from: 'app/img/favicon.ico'},
      {from: 'app/img/browsers/chrome.png'},
      {from: 'app/img/browsers/firefox.png'},
      {from: 'app/img/browsers/safari.png'},
      {from: 'app/img/logo.svg'},
      {from: 'app/img/icons/bucket.svg'},
      {from: 'app/img/icons/search.svg'},
      {from: 'app/img/icons/info.svg'},
      {from: 'app/img/icons/settings.svg'},
      {from: 'app/img/icons/fullscreen.svg'},
      {from: 'app/img/icons/fullscreen-exit.svg'},
      {from: 'app/img/icons/more-h.svg'},
      {from: 'app/img/icons/github.svg'},
      {from: 'app/img/icons/chevron-sm.svg'},
      {from: 'app/img/icons/item-selected.svg'},
      {from: 'app/img/icons/trash.svg'},
      {from: 'app/img/icons/download.svg'},
      {from: 'app/img/icons/times.svg'},
      {from: 'app/img/icons/check.svg'},
      {from: 'app/img/icons/folder.svg'},
      {from: 'app/img/icons/copy.svg'},
      {from: 'app/img/icons/times-white.svg'},
      {from: 'app/img/icons/bucket-sm.svg'},
      {from: 'app/img/icons/folder-sm.svg'},
      {from: 'app/img/icons/upload-sm.svg'},
      {from: 'app/img/icons/visible.svg'},
      {from: 'app/img/icons/invisible.svg'},
      {from: 'app/img/icons/help.svg'},
      {from: 'app/img/icons/docs.svg'},
      {from: 'app/img/icons/off.svg'},
      {from: 'app/img/icons/warning.svg'},
      {from: 'app/img/icons/chevron-up.svg'},
      {from: 'app/img/icons/arrow-right.svg'},
      {from: 'app/index.html'}
    ]),
    new webpack.ContextReplacementPlugin(/moment[\\\/]locale$/, /^\.\/(en)$/),
    new purify({
        basePath: __dirname,
        paths: [
            "app/index.html",
            "app/js/*.js"
        ]
    })
  ]
}

if (process.env.NODE_ENV === 'dev') {
  exports.entry = [
    'webpack/hot/dev-server',
    'webpack-dev-server/client?http://localhost:8080',
    path.resolve(__dirname, 'app/index.js')
  ]
}

module.exports = exports
