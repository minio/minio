#!/bin/sh

# Create buckets with versioning and object locking enabled.
mc mb -l source/bucket
mc mb -l dest/bucket

#### Create a replication admin on source alias
# create a replication admin user : repladmin
mc admin user add source repladmin repladmin123

# create a replication policy for repladmin
cat >repladmin-policy-source.json <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
    {
        "Action": [
            "admin:SetBucketTarget",
            "admin:GetBucketTarget"
        ],
        "Effect": "Allow",
        "Sid": ""
     }, 
     {
      "Effect": "Allow",
      "Action": [
       "s3:GetReplicationConfiguration",
       "s3:PutReplicationConfiguration",
       "s3:ListBucket",
       "s3:ListBucketMultipartUploads",
       "s3:GetBucketLocation",
       "s3:GetBucketVersioning"
      ],
      "Resource": [
       "arn:aws:s3:::bucket"
      ]
     }
    ]
   }
EOF
mc admin policy create source repladmin-policy ./repladmin-policy-source.json
cat ./repladmin-policy-source.json

#assign this replication policy to repladmin
mc admin policy attach source repladmin-policy --user=repladmin

### on dest alias
# Create a replication user : repluser on dest alias
mc admin user add dest repluser repluser123

# create a replication policy for repluser
# Remove "s3:GetBucketObjectLockConfiguration" if object locking is not enabled, i.e. bucket was not created with `mc mb --with-lock` option
# Remove "s3:ReplicateDelete" if delete marker replication is not required
cat >replpolicy.json <<EOF
{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "s3:GetReplicationConfiguration",
    "s3:ListBucket",
    "s3:ListBucketMultipartUploads",
    "s3:GetBucketLocation",
    "s3:GetBucketVersioning",
    "s3:GetBucketObjectLockConfiguration"
   ],
   "Resource": [
    "arn:aws:s3:::bucket"
   ]
  },
  {
   "Effect": "Allow",
   "Action": [
    "s3:GetReplicationConfiguration",
    "s3:ReplicateTags",
    "s3:AbortMultipartUpload",
    "s3:GetObject",
    "s3:GetObjectVersion",
    "s3:GetObjectVersionTagging",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ReplicateObject",
    "s3:ReplicateDelete"
   ],
   "Resource": [
    "arn:aws:s3:::bucket/*"
   ]
  }
 ]
}
EOF
mc admin policy create dest replpolicy ./replpolicy.json
cat ./replpolicy.json

# assign this replication policy to repluser
mc admin policy attach dest replpolicy --user=repluser

# configure replication config to remote bucket at http://localhost:9000
mc replicate add source/bucket --priority 1 --remote-bucket http://repluser:repluser123@localhost:9000/bucket \
	--replicate existing-objects,delete,delete-marker,replica-metadata-sync
