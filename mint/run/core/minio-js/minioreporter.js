/*
 * Minio Reporter for JSON formatted logging, (C) 2017 Minio, Inc.
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

var mocha = require('mocha');
module.exports = minioreporter;

function minioreporter(runner) {
  mocha.reporters.Base.call(this, runner);
   var self = this;

  runner.on('pass', function (test) {
    GenerateJsonEntry(test)
  });

  runner.on('fail', function (test, err) {
    GenerateJsonEntry(test, err)
  });

}

/**
 * Convert test result into a JSON object and print on the console.
 *
 * @api private
 * @param test, err
 */

function GenerateJsonEntry (test, err) {
  var res = test.title.split("_")
  var jsonEntry = {};

  jsonEntry.name = "minio-js"  
  
  if (res.length > 0 && res[0].length) {
    jsonEntry.function = res[0]
  }
  
  if (res.length > 1 && res[1].length) {
    jsonEntry.args = res[1]
  }

  jsonEntry.duration = test.duration
  
  if (res.length > 2 && res[2].length) {
    jsonEntry.alert = res[2]
  }

  if (err != null ) {
    jsonEntry.status = "FAIL"
    jsonEntry.error = err.stack.replace(/\n/g, " ").replace(/ +(?= )/g,'')
  } else {
    jsonEntry.status = "PASS"
  }

  process.stdout.write(JSON.stringify(jsonEntry) + "\n")
}
