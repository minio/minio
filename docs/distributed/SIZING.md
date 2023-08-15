# Erasure code sizing guide

## Toy Setups

Capacity constrained environments, MinIO will work but not recommended for production.

| servers | drives (per node) | stripe_size | parity chosen (default) | tolerance for reads (servers) | tolerance for writes (servers) |
|--------:|------------------:|------------:|------------------------:|------------------------------:|-------------------------------:|
|       1 |                 1 |           1 |                       0 |                             0 |                              0 |
|       1 |                 4 |           4 |                       2 |                             0 |                              0 |
|       4 |                 1 |           4 |                       2 |                             2 |                              1 |
|       5 |                 1 |           5 |                       2 |                             2 |                              2 |
|       6 |                 1 |           6 |                       3 |                             3 |                              2 |
|       7 |                 1 |           7 |                       3 |                             3 |                              3 |

## Minimum System Configuration for Production

| servers | drives (per node) | stripe_size | parity chosen (default) | tolerance for reads (servers) | tolerance for writes (servers) |
|--------:|------------------:|------------:|------------------------:|------------------------------:|-------------------------------:|
|       4 |                 2 |           8 |                       4 |                             2 |                              1 |
|       5 |                 2 |          10 |                       4 |                             2 |                              2 |
|       6 |                 2 |          12 |                       4 |                             2 |                              2 |
|       7 |                 2 |          14 |                       4 |                             2 |                              2 |
|       8 |                 1 |           8 |                       4 |                             4 |                              3 |
|       8 |                 2 |          16 |                       4 |                             2 |                              2 |
|       9 |                 2 |           9 |                       4 |                             4 |                              4 |
|      10 |                 2 |          10 |                       4 |                             4 |                              4 |
|      11 |                 2 |          11 |                       4 |                             4 |                              4 |
|      12 |                 2 |          12 |                       4 |                             4 |                              4 |
|      13 |                 2 |          13 |                       4 |                             4 |                              4 |
|      14 |                 2 |          14 |                       4 |                             4 |                              4 |
|      15 |                 2 |          15 |                       4 |                             4 |                              4 |
|      16 |                 2 |          16 |                       4 |                             4 |                              4 |

If one or more drives are offline at the start of a PutObject or NewMultipartUpload operation the object will have additional data
protection bits added automatically to provide the regular safety for these objects up to 50% of the number of drives.
This will allow normal write operations to take place on systems that exceed the write tolerance.

This means that in the examples above the system will always write 4 parity shards at the expense of slightly higher disk usage.
