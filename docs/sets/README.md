Introduction [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
------------

This feature allows Minio to combine a set of disks larger than 16 in a distributed setup. There are no special configuration changes required to enable this feature. Access to files stored across this setup are locked and synchronized by default.

Motivation
----------

As next-generation data centers continue to shrink, IT professions must re-evaluate ahead to get the benefits of greater server density and storage density. Computer hardware is changing rapidly in system form factors, virtualization, containerization have allowed far more enterprise computing with just a fraction of the physical space. Increased densities allow for smaller capital purchases and lower energy bills.

Restrictions
------------

* Each set is still a maximum of 16 disks, you can start with multiple such sets statically.
* Static sets of disks and cannot be changed, there is no elastic expansion allowed.
* ListObjects() across sets can be relatively slower since List happens on all servers, and is merged at this layer.
