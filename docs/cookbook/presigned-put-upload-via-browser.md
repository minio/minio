# Upload files from browser using pre-signed URLs [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Using presigned URLs, you can allow a browser to upload a file
directly to S3 without exposing your S3 credentials to the user. The following
is an annotated example of this using [minio-js](https://github.com/minio/minio-js).

### Server code

```javascript
const Minio = require('minio')

var client = new Minio({
    endPoint: 'play.minio.io',
    port: 9000,
    secure: true,
    accessKey: 'Q3AM3UQ867SPQQA43P2F',
    secretKey: 'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG'
})
```

Here, we create a new Minio client, which is necessary in order to presign
an upload URL.

Those are real Minio server credentials — try them out!

```javascript
// express is a small HTTP server wrapper, but this works with any HTTP server
const server = require('express')()
// uuid.v4() generates a unique identifier for each upload
const uuid = require('node-uuid')

server.get('/presignedUrl', (req, res) => {

    client.presignedPutObject('uploads', uuid.v4(), (err, url) => {
        if (err) throw err

        res.end(url)
    })

})

server.listen(8080)
```

Here are the docs for [`presignedPutObject`](https://docs.minio.io/docs/javascript-client-api-reference#presignedPutObject).

When a user visits the server, it will respond with a URL that can be used to
upload a file to S3. Then, using AJAX requests, you can upload a file
straight from the browser.

### Client code

This application uses [jQuery](http://jquery.com/).

On the browser, we want to allow the user to select a file for upload, then,
after retrieving an upload URL from the Node.js server, the user can upload it
straight to S3.

```xml
<input type="file" id="selector" multiple>
<button onclick="upload()">Upload</button>

<div id="status">No uploads</div>

<script src="https://code.jquery.com/jquery-3.1.0.min.js"></script>
<script type="text/javascript">

function upload() {
    let files = $('#selector')[0].files
    files.forEach(file => {
        // Retrieve a URL from our server.
        retrieveNewURL(url => {
            // Upload the file to the server.
            uploadFile(file, url)
        })
    })
}

// Request to our Node.js server for an upload URL.
function retrieveNewURL(cb) {
    $.get('http://YOUR_SERVER:8080/presignUrl', (url) => {
        cb(url)
    })
}

// Use AJAX to upload the file to S3.
function uploadFile(file, url) {
    let data = new FormData()
    data.append(file.name, file)

    $.ajax({
        url: url,
        method: 'PUT',
        success: () => {
            $('#status').text(`Uploaded ${file.name}.`)
        },
        processData: false,
        contentType: false,
        data
    })
}

</script>
```

Now a user can visit the website and upload a file straight to S3, without
exposing the S3 credentials.
