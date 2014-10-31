Git Workflow
============

Update local repo with latest changes from upstream
```sh
git fetch
```

Create a new branch from the latest code
```sh
git checkout origin/master
git checkout -b new_feature_branch
```

```sh
# do work here
```

Create commit for submission
```sh
git commit -m "My Commit Message"
```

Prepare commit for inclusion
```sh
git fetch
git rebase origin/master
```

Assuming no conflict, push to your personal fork.

```sh
git push myrepo new_feature_branch:new_feature_branch
# Visit https://github.com/minios/minios and create a new pull request
from your branch.
```

Useful Tools
------------
As an alternative to manually pushing and creating a branch, use github.com/docker/gordon pulls send command:

Create a new pull request.
```sh
pulls send
# automatically performs git push and creates pull request
```

Update an existing pull request (e.g. PR 42)
```sh
pulls send 42
```
