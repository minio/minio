# MinIO Pull Request Guidelines

These guidelines ensure high-quality commits in MinIO’s GitHub repositories, maintaining 
a clear, valuable commit history for our open-source projects. They apply to all contributors, 
fostering efficient reviews and robust code.

## Why Pull Requests?

Pull Requests (PRs) drive quality in MinIO’s codebase by:
- Enabling peer review without pair programming.
- Documenting changes for future reference.
- Ensuring commits tell a clear story of development.

**A poor commit lasts forever, even if code is refactored.**

## Crafting a Quality PR

A strong MinIO PR:
- Delivers a complete, valuable change (feature, bug fix, or improvement).
- Has a concise title (e.g., `[S3] Fix bucket policy parsing #1234`) and a summary with context, referencing issues (e.g., `#1234`).
- Contains well-written, logical commits explaining *why* changes were made (e.g., “Add S3 bucket tagging support so that users can organize resources efficiently”).
- Is small, focused, and easy to review—ideally one commit, unless multiple commits better narrate complex work.
- Adheres to MinIO’s coding standards (e.g., Go style, error handling, testing).

PRs must flow smoothly through review to reach production. Large PRs should be split into smaller, manageable ones.

## Submitting PRs

1. **Title and Summary**:
   - Use a scannable title: `[Subsystem] Action Description #Issue` (e.g., `[IAM] Add role-based access control #567`).
   - Include context in the summary: what changed, why, and any issue references.
   - Use `[WIP]` for in-progress PRs to avoid premature merging or choose GitHub draft PRs.

2. **Commits**:
   - Write clear messages: what changed and why (e.g., “Refactor S3 API handler to reduce latency so that requests process 20% faster”).
   - Rebase to tidy commits before submitting (e.g., `git rebase -i main` to squash typos or reword messages), unless multiple contributors worked on the branch.
   - Keep PRs focused—one feature or fix. Split large changes into multiple PRs.

3. **Testing**:
   - Include unit tests for new functionality or bug fixes.
   - Ensure existing tests pass (`make test`).
   - Document testing steps in the PR summary if manual testing was performed.

4. **Before Submitting**:
   - Run `make verify` to check formatting, linting, and tests.
   - Reference related issues (e.g., “Closes #1234”).
   - Notify team members via GitHub `@mentions` if urgent or complex.

## Reviewing PRs

Reviewers ensure MinIO’s commit history remains a clear, reliable record. Responsibilities include:

1. **Commit Quality**:
   - Verify each commit explains *why* the change was made (e.g., “So that…”).
   - Request rebasing if commits are unclear, redundant, or lack context (e.g., “Please squash typo fixes into the parent commit”).

2. **Code Quality**:
   - Check adherence to MinIO’s Go standards (e.g., error handling, documentation).
   - Ensure tests cover new code and pass CI.
   - Flag bugs or critical issues for immediate fixes; suggest non-blocking improvements as follow-up issues.

3. **Flow**:
   - Review promptly to avoid blocking progress.
   - Balance quality and speed—minor issues can be addressed later via issues, not PR blocks.
   - If unable to complete the review, tag another reviewer (e.g., `@username please take over`).

4. **Shared Responsibility**:
   - All MinIO contributors are reviewers. The first commenter on a PR owns the review unless they delegate.
   - Multiple reviewers are encouraged for complex PRs.

5. **No Self-Edits**:
   - Don’t modify the PR directly (e.g., fixing bugs). Request changes from the submitter or create a follow-up PR.
   - If you edit, you’re a collaborator, not a reviewer, and cannot merge.

6. **Testing**:
   - Assume the submitter tested the code. If testing is unclear, ask for details (e.g., “How was this tested?”).
   - Reject untested PRs unless testing is infeasible, then assist with test setup.

## Tips for Success

- **Small PRs**: Easier to review, faster to merge. Split large changes logically.
- **Clear Commits**: Use `git rebase -i` to refine history before submitting.
- **Engage Early**: Discuss complex changes in issues or Slack (https://slack.min.io) before coding.
- **Be Responsive**: Address reviewer feedback promptly to keep PRs moving.
- **Learn from Reviews**: Use feedback to improve future contributions.

## Resources

- [MinIO Coding Standards](https://github.com/minio/minio/blob/master/CONTRIBUTING.md)
- [Effective Commit Messages](https://mislav.net/2014/02/hidden-documentation/)
- [GitHub PR Tips](https://github.com/blog/1943-how-to-write-the-perfect-pull-request)

By following these guidelines, we ensure MinIO’s codebase remains high-quality, maintainable, and a joy to contribute to. Happy coding!
