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

var webpack = require('webpack')
var path = require('path')
var glob = require('glob-all')
var CopyWebpackPlugin = require('copy-webpack-plugin')
var PurgecssPlugin = require('purgecss-webpack-plugin')

var exports = {
  context: __dirname,
  mode: 'production',
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
    new CopyWebpackPlugin({patterns: [
      {from: 'app/css/loader.css'},
      {from: 'app/img/browsers/chrome.png'},
      {from: 'app/img/browsers/firefox.png'},
      {from: 'app/img/browsers/safari.png'},
      {from: 'app/img/logo.svg'},
      {from: 'app/img/favicon/favicon-16x16.png'},
      {from: 'app/img/favicon/favicon-32x32.png'},
      {from: 'app/img/favicon/favicon-96x96.png'},
      {from: 'app/index.html'}
    ]}),
    new webpack.ContextReplacementPlugin(/moment[\\\/]locale$/, /^\.\/(en)$/),
    new PurgecssPlugin({
      paths: glob.sync([
        path.join(__dirname, 'app/index.html'),
        path.join(__dirname, 'app/js/*.js')
      ])
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
