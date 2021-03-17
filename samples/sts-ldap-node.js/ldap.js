const axios = require('axios');

const url = 'http://localhost:9000';
const params = new URLSearchParams();
params.append('Action', 'AssumeRoleWithLDAPIdentity');
params.append('LDAPUsername', 'Philip J. Fry');
params.append('LDAPPassword', 'fry');
params.append('Version', '2011-06-15');

const config = {
  headers: {
    'Content-Type': 'application/x-www-form-urlencoded'
  }
};

axios
  .post(url, params, config)
  .then(response => {
    console.log(response);
    // eslint-disable-next-line global-require
    const xml2js = require('xml2js');
    const parser = new xml2js.Parser({
      explicitRoot: false,
      explicitArray: false,
      ignoreAttrs: true
    });
    parser
      .parseStringPromise(response.data)
      .then(result => {
        console.dir(result);
        const accessKeyId =
          result.AssumeRoleWithLDAPIdentityResult.Credentials.AccessKeyId;
        const secretAccessKey =
          result.AssumeRoleWithLDAPIdentityResult.Credentials.SecretAccessKey;
        const sessionToken =
          result.AssumeRoleWithLDAPIdentityResult.Credentials.SessionToken;

        // eslint-disable-next-line global-require
        const Minio = require('minio');

        const minioClient = new Minio.Client({
          endPoint: 'localhost',
          port: 9000,
          useSSL: false,
          accessKey: accessKeyId,
          secretKey: secretAccessKey,
          sessionToken
        });

        minioClient.listBuckets((err, buckets) => {
          if (err) return console.log(err);
          console.log('buckets :', buckets);
        });

        return true;
      })
      .catch(err => {
        console.log(err);
      });
    return true;
  })
  .catch(error => {
    console.log(error);
  });
