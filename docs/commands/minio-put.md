% MINIO(1) Minio Manual
% Minio
% December 2014
# NAME
minio-put - Put an object into the store.

# SYNOPSIS
**minio put**
BUCKET OBJECTPATH [FILE]

# DESCRIPTION
Adds an object to a given BUCKET at the given OBJECTPATH. An optional FILE may
be provided. If no FILE is provided, the standard input stream (STDIN) is used
instead.

# EXAMPLES

    $ minio put images /favorites/example.png local_image.png
    $ minio put images /favorites/example2.png < local_image2.png
