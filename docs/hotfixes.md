# Introduction

This document outlines how to make hotfix binaries and containers for MinIO?. The main focus in this article is about how to backport patches to a specific branch and finally building binaries/containers.

## Pre-pre requisite

- A working knowledge of MinIO codebase and its various components.
- A working knowledge of AWS S3 API behaviors and corner cases.

## Pre-requisite for backporting any fixes

Fixes that are allowed a backport must satisfy any of the following criteria's:

- A fix must not be a feature, for example.

```
commit faf013ec84051b92ae0f420a658b8d35bb7bb000
Author: Klaus Post <klauspost@gmail.com>
Date:   Thu Nov 18 12:15:22 2021 -0800

    Improve performance on multiple versions (#13573)
```

- A fix must be a valid fix that was reproduced and seen in a customer environment, for example.

```
commit 886262e58af77ebc7c836ef587c08544e9a0c271
Author: Harshavardhana <harsha@minio.io>
Date:   Wed Nov 17 15:49:12 2021 -0800

    heal legacy objects when versioning is enabled after upgrade (#13671)
```

- A security fix must be backported if a customer is affected by it, we have a mechanism in SUBNET to send out notifications to affected customers in such situations, this is a mandatory requirement.

```
commit 99bf4d0c429f04dbd013ba98840d07b759ae1702 (tag: RELEASE.2019-06-15T23-07-18Z)
Author: Harshavardhana <harsha@minio.io>
Date:   Sat Jun 15 11:27:17 2019 -0700

    [security] Match ${aws:username} exactly instead of prefix match (#7791)

    This PR fixes a security issue where an IAM user based
    on his policy is granted more privileges than restricted
    by the users IAM policy.

    This is due to an issue of prefix based Matcher() function
    which was incorrectly matching prefix based on resource
    prefixes instead of exact match.
```

- There is always a possibility of a fix that is new, it is advised that the developer must make sure that the fix is sent upstream, reviewed and merged to the master branch.

## Creating a hotfix branch

Customers in MinIO are allowed LTS on any release they choose to standardize. Production setups seldom change and require maintenance. Hotfix branches are such maintenance branches that allow customers to operate a production cluster without drastic changes to their deployment.

## Backporting a fix

Developer is advised to clone the MinIO source and checkout the MinIO release tag customer is currently on.

```
λ git checkout RELEASE.2021-04-22T15-44-28Z
```

Create a branch and proceed to push the branch **upstream**
> (upstream here points to git@github.com:minio/minio.git)

```
λ git branch -m RELEASE.2021-04-22T15-44-28Z.hotfix
λ git push -u upstream RELEASE.2021-04-22T15-44-28Z.hotfix
```

Pick the relevant commit-id say for example commit-id from the master branch

```
commit 4f3317effea38c203c358af9cb5ce3c0e4173976
Author: Klaus Post <klauspost@gmail.com>
Date:   Mon Nov 8 08:41:27 2021 -0800

    Close stream on panic (#13605)

    Always close streamHTTPResponse on panic on main thread to avoid
    write/flush after response handler has returned.
```

```
λ git cherry-pick 4f3317effea38c203c358af9cb5ce3c0e4173976
```

*A self contained **patch** usually applies fine on the hotfix branch during backports as long it is self contained. There are situations however this may lead to conflicts and the patch will not cleanly apply. Conflicts might be trivial which can be resolved easily, when conflicts seem to be non-trivial or touches the part of the code-base the developer is not confident - to get additional clarity reach out to #hack on MinIOHQ slack channel. Hasty changes must be avoided, minor fixes and logs may be added to hotfix branches but this should not be followed as practice.*

Once the **patch** is successfully applied, developer must run tests to validate the fix that was backported by running following tests, locally.

Unit tests

```
λ make test
```

Verify different type of MinIO deployments work

```
λ make verify
```

Verify if healing and replacing a drive works

```
λ make verify-healing
```

At this point in time the backport is ready to be submitted as a pull request to the relevant branch. A pull request is recommended to ensure [mint](http://github.com/minio/mint) tests are validated. Pull request also ensures code-reviews for the backports in case of any unforeseen regressions.

### Building a hotfix binary and container

To add a hotfix tag to the binary version and embed the relevant `commit-id` following build helpers are available

#### Builds the hotfix binary and uploads to https;//dl.min.io

```
λ CRED_DIR=/media/builder/minio make hotfix-push
```

#### Builds the hotfix container and pushes to docker.io/minio/minio

```
λ CRED_DIR=/media/builder/minio make docker-hotfix-push
```

#### Builds the hotfix container and pushes to registry.min.dev/<customer>/minio

```
λ REPO="registry.min.dev/<customer>" CRED_DIR=/media/builder/minio make docker-hotfix-push
```

Once this has been provided to the customer relevant binary will be uploaded from our *release server* securely, directly to <https://dl.minio.io/server/minio/hotfixes/archive/>
