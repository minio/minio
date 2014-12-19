% MINIO(1) Minio Manual
% Minio
% December 2014
# NAME
minio-get - Get an object from the store.

# SYNOPSIS
**minio get**
BUCKET OBJECTPATH

# DESCRIPTION
Gets an object from a given BUCKET at the given OBJECTPATH. The object is
returned on the standard output stream (STDOUT)

# EXAMPLES

    $ minio get images /favorites/example.png > local_image.png
