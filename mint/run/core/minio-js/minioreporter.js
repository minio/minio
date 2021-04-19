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
