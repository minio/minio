# Server command line SPEC. - Tue Mar 22 06:04:42 UTC 2016

Minio initialize filesystem backend.
~~~
$ minio init fs <path>
~~~

Minio initialize XL backend.
~~~
$ minio init xl <url1>...<url16>
~~~

For 'fs' backend it starts the server.
~~~
$ minio server
~~~

For 'xl' backend it waits for servers to join.
~~~
$ minio server
... [PROGRESS BAR] of servers connecting
~~~

Now on other servers execute 'join' and they connect.
~~~
....
minio join <url1> -- from <url2> && minio server
minio join <url1> -- from <url3> && minio server
...
...
minio join <url1> -- from <url16> && minio server
~~~
