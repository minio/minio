#!/bin/bash

#######################################################################
# This script is for configuring your IBM Messaging Appliance for use #
# as an mqtt test server for testing the go-mqtt open source client.  #
# It creates the Policies and Endpoints necessary to test particular  #
# features of the client, such as IPv6, SSL, and other things         #
#                                                                     #
# You do not need this script for any other purpose.                  #
#######################################################################

# Edit options to match your configuration
IMA_HOST=9.41.55.184
IMA_USER=admin
HOST=9.41.55.146
USER=root
CERTDIR=~/GO/src/github.com/shoenig/go-mqtt/samples/samplecerts

echo 'Configuring your IBM Messaging Appliance for testing go-mqtt'
echo 'IMA_HOST: ' $IMA_HOST


function ima {
    reply=`ssh $IMA_USER@$IMA_HOST imaserver $@`
}

function imp {
    reply=`ssh $IMA_USER@$IMA_HOST file get $@`
}

ima create MessageHub Name=GoMqttTestHub

# Config "1" is a basic, open endpoint, port 17001
ima create MessagingPolicy \
    Name=GoMqttMP1 \
    Protocol=MQTT \
    ActionList=Publish,Subscribe \
    MaxMessages=100000 \
    DestinationType=Topic \
    Destination=*

ima create ConnectionPolicy \
    Name=GoMqttCP1 \
    Protocol=MQTT

ima create Endpoint \
    Name=GoMqttEP1 \
    Protocol=MQTT \
    MessageHub=GoMqttTestHub \
    ConnectionPolicies=GoMqttCP1 \
    MessagingPolicies=GoMqttMP1 \
    Port=17001

# Config "2" is IPv6 only , port 17002

# Config "3" is for authorization failures, port 17003
ima create ConnectionPolicy \
    Name=GoMqttCP2 \
    Protocol=MQTT \
    ClientID=GoMqttClient

ima create Endpoint \
    Name=GoMqttEP3 \
    Protocol=MQTT \
    MessageHub=GoMqttTestHub \
    ConnectionPolicies=GoMqttCP2 \
    MessagingPolicies=GoMqttMP1 \
    Port=17003

# Config "4" is secure connections, port 17004
imp scp://$USER@$HOST:${CERTDIR}/server-crt.pem .
imp scp://$USER@$HOST:${CERTDIR}/server-key.pem .
imp scp://$USER@$HOST:${CERTDIR}/rootCA-crt.pem .
imp scp://$USER@$HOST:${CERTDIR}/intermediateCA-crt.pem .

ima apply Certificate \
    CertFileName=server-crt.pem \
    "CertFilePassword=" \
    KeyFileName=server-key.pem \
    "KeyFilePassword="

ima create CertificateProfile \
    Name=GoMqttCertProf \
    Certificate=server-crt.pem \
    Key=server-key.pem

ima create SecurityProfile \
    Name=GoMqttSecProf \
    MinimumProtocolMethod=SSLv3 \
    UseClientCertificate=True \
    UsePasswordAuthentication=False \
    Ciphers=Fast \
    CertificateProfile=GoMqttCertProf

ima apply Certificate \
    TrustedCertificate=rootCA-crt.pem \
    SecurityProfileName=GoMqttSecProf

ima apply Certificate \
    TrustedCertificate=intermediateCA-crt.pem \
    SecurityProfileName=GoMqttSecProf

ima create Endpoint \
    Name=GoMqttEP4 \
    Port=17004 \
    MessageHub=GoMqttTestHub \
    ConnectionPolicies=GoMqttCP1 \
    MessagingPolicies=GoMqttMP1 \
    SecurityProfile=GoMqttSecProf \
    Protocol=MQTT

