# Using pre-signed URLs to download via the browser [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Using presigned URLs, you can allow a browser to download a private file
directly from S3 without exposing your S3 credentials to the user. The
following is an annotated example of how this can be used in a Node.js
application, using [minio-js](https://github.com/minio/minio-js).

This application will work out of the box, just copy each piece together into a
file and run it.

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

In order to sign the request, we need to create the Minio client and pass it
our credentials. With the client, you can download and upload files,
among [much more](https://github.com/minio/minio-js/blob/master/docs/API.md).

These are real credentials to an example Minio server — try it out!

```javascript
// express is a small HTTP server wrapper, but this works with any HTTP server
const server = require('express')()

server.get('/presignedUrl', (req, res) => {

    client.presignedGetObject('pictures', 'house.png', (err, url) => {
        if (err) throw err

        res.redirect(url)
    })

})

server.listen(8080)
```

[`presignedGetObject`](https://docs.minio.io/docs/javascript-client-api-reference#presignedGetObject)
creates the URL we can use to download `pictures/house.png`. The link will
automatically expire after 7 days — this can be adjusted using the optional
`expiry` argument.

In this example, the HTTP server will generate a link to download an image of
a house from S3. It will then redirect the user to that link.

### Client code
You can also use client-side JavaScript to request the file from the server.
Using [jQuery](http://jquery.com/), here is an example of this in action. It
will insert some text from S3 into a `div`.

```xml
<div id="response"></div>

<script src="https://code.jquery.com/jquery-3.1.0.min.js"></script>
<script defer type="text/javascript">

$.get('http://YOUR_SERVER:8080/presignedUrl', (text) => {
	// Set the text.
	$('#response').text(text)
}, 'string')

</script>
```
