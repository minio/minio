/*
 * MinIO Cloud Storage (C) 2016 MinIO, Inc.
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
var glob = require('glob-all')
var CopyWebpackPlugin = require('copy-webpack-plugin')
var PurgecssPlugin = require('purgecss-webpack-plugin')

var exports = {
  context: __dirname,
  mode: 'development',
  entry: [
    path.resolve(__dirname, 'app/index.js')
  ],
  output: {
    path: path.resolve(__dirname, 'dev'),
    filename: 'index_bundle.js',
    publicPath: '/minio/'
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
  devServer: {
    historyApiFallback: {
      index: '/minio/'
    },
    proxy: {
      '/minio/webrpc': {
        target: 'http://localhost:9000',
        secure: false,
        headers: {'Host': "localhost:9000"}
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
