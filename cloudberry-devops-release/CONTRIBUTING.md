Apache Cloudberry welcomes contributions from anyone, new and
experienced! We appreciate your interest in contributing. This guide
will help you get started with the contribution.

## Code of Conduct

Everyone who participates in Cloudberry, either as a user or a
contributor, is obliged to follow the ASF Code of Conduct.

## GitHub Contribution Workflow

1. Fork this repo to your own GitHub account.
2. Clone down the repo to your local system.

```
git clone https://github.com/your-user-name/cloudberry-devops-release.git
```

3. Add the upstream repo. (You only have to do this once, not every time.)

```
git remote add upstream https://github.com/apache/cloudberry-devops-release.git
```

4. Create a new branch to hold your work.

```
git checkout -b new-branch-name
```

5. Work on your new code.

6. Commit your changes.

```
git add <the change files>
git commit
```

7. Push your changes to your GitHub repo.

```
git push origin new-branch-name
```

8. Open a PR(Pull Request).

Go to the repo on GitHub. There will be a message about your recently pushed
branch, asking if you would like to open a pull request. Follow the prompts,
compare across repositories, and submit the PR.

9. Get your code reviewed.

10. Congratulations! Once your PR is approved, then the code will be
merged. Your code will be shipped in the recent future releases.

## Sync your branch with the upstream

Before working on your next contribution, make sure your local repository is
up to date:

```
git checkout main
git fetch upstream
git rebase upstream/main
```
