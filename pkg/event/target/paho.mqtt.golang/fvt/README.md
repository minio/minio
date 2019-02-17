FVT Instructions
================

The FVT tests are currenly only supported by [IBM MessageSight](http://www-03.ibm.com/software/products/us/en/messagesight/).

Support for [mosquitto](http://mosquitto.org/) and [IBM Really Small Message Broker](https://www.ibm.com/developerworks/community/groups/service/html/communityview?communityUuid=d5bedadd-e46f-4c97-af89-22d65ffee070) might be added in the future.


IBM MessageSight Configuration
------------------------------

The IBM MessageSight Virtual Appliance can be downloaded here:
[Download](http://www-933.ibm.com/support/fixcentral/swg/selectFixes?parent=ibm~Other+software&product=ibm/Other+software/MessageSight&function=fixId&fixids=1.0.0.1-IMA-DeveloperImage&includeSupersedes=0 "IBM MessageSight")

There is a nice blog post about it here:
[Blog](https://www.ibm.com/developerworks/community/blogs/c565c720-fe84-4f63-873f-607d87787327/entry/ibm_messagesight_for_developers_is_here?lang=en "Blog")


The virtual appliance must be installed into a virtual machine like
Oracle VirtualBox or VMWare Player. (Follow the instructions that come
with the download).

Next, copy your authorized keys (basically a file containing the public
rsa key of your own computer) onto the appliance to enable passwordless ssh.

For example,

    Console> user sshkey add "scp://user@host:~/.ssh/authorized_keys"

More information can be found in the IBM MessageSight InfoCenter:
[InfoCenter](https://infocenters.hursley.ibm.com/ism/v1/help/index.jsp "InfoCenter")

Now, execute the script setup_IMA.sh to create the objects necessary
to configure the server for the unit test cases provided.

For example,

    ./setup_IMA.sh

You should now be able to view the objects on your server:

    Console> imaserver show Endpoint Name=GoMqttEP1
    Name = GoMqttEP1
    Enabled = True
    Port = 17001
    Protocol = MQTT
    Interface = all
    SecurityProfile =
    ConnectionPolicies = GoMqttCP1
    MessagingPolicies = GoMqttMP1
    MaxMessageSize = 1024KB
    MessageHub = GoMqttTestHub
    Description =



RSMB Configuration
------------------
Wait for SSL support?


Mosquitto Configuration
-----------------------
Launch mosquitto from the fvt directory, specifiying mosquitto.cfg as config file

``ex: /usr/bin/mosquitto -c ./mosquitto.cfg``

Note: Mosquitto requires SSL 1.1 or better, while Go 1.1.2 supports
only SSL v1.0. However, Go 1.2+ supports SSL v1.1 and SSL v1.2.


Other Notes
-----------
Go 1.1.2 does not support intermediate certificates, however Go 1.2+ does.
