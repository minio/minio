var webpack = require('webpack');

module.exports = function (config) {
  config.set({
    browsers: [ process.env.CONTINUOUS_INTEGRATION ? 'Firefox' : 'Chrome' ],
    singleRun: true,
    frameworks: [ 'mocha' ],
    files: [
      'tests.webpack.js'
    ],
    preprocessors: {
      'tests.webpack.js': [ 'webpack' ]
    },
    reporters: [ 'dots' ],
    webpack: {
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
          test: /\.css$/,
          loader: 'style!css'
        }, {
          test: /\.(eot|woff|woff2|ttf|svg|png)/,
          loader: 'url'
        }]
      }
    },
    webpackServer: {
      noInfo: true
    }
  });
};
