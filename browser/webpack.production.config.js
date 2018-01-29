/*
 * Isomorphic Javascript library for Minio Browser JSON-RPC API, (C) 2016 Minio, Inc.
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
    path: path.resolve(__dirname, 'production'),
    filename: 'index_bundle.js'
  },
  module: {
    rules: [{
        test: /\.js$/,
        exclude: /(node_modules|bower_components)/,
        use: [{
          loader: 'babel-loader',
          options: {
            presets: ['react', 'es2015']
          }
        }]
      }, {
        test: /\.less$/,
        use: [{
          loader: 'style-loader'
        }, {
          loader: 'css-loader'
        }, {
          loader: 'less-loader'
        }]
      }, {
        test: /\.css$/,
        use: [{
          loader: 'style-loader'
        }, {
          loader: 'css-loader'
        }]
      }, {
        test: /\.(eot|woff|woff2|ttf|svg|png)/,
        use: [{
          loader: 'url-loader'
        }]
      }]
  },
  node:{
    fs:'empty'
  },
  plugins: [
    new CopyWebpackPlugin([
      {from: 'app/css/loader.css'},
      {from: 'app/img/favicon.ico'},
      {from: 'app/img/browsers/chrome.png'},
      {from: 'app/img/browsers/firefox.png'},
      {from: 'app/img/browsers/safari.png'},
      {from: 'app/img/logo.svg'},
      {from: 'app/index.html'}
    ]),
    new webpack.DefinePlugin({
        'process.env.NODE_ENV': '"production"'
    }),
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
    'webpack-dev-server/client?http://localhost:8080',
    path.resolve(__dirname, 'app/index.js')
  ]
}

module.exports = exports
