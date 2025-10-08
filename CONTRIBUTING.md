# Contributing

Greengage was launched and is maintained by a team of independent contributors to Greenplum. Currently, we own the [Greengage DB repository on GitHub](https://github.com/GreengageDB/greengage), appoint the members of the architectural committee and perform other work related to the organizational process in addition to software development. However, great open source projects cannot exist and last long without a strong community, **so community contributions to Greengage DB are very welcome**. In this section, you’ll find initial guidance on how to contribute to Greengage and links to additional resources that will help you get your code released as part of Greengage.

## Getting started

To contribute to Greengage, you will need a [GitHub account](https://github.com/signup). If you haven’t used Git before, find a time to familiarize yourself with [Git tooling and workflow](https://wiki.postgresql.org/wiki/Working_with_Git) before you start.

A private copy of the Greengage repository is required to introduce changes. To create one, [fork](https://github.com/GreengageDB/greengage/fork) our repository and work on it. Having changed something, you will be able to pull request, and someone from the architectural committee will review your contribution. To get more information on the review process, see the “Patch review” section of this document.

## Contributions licensing

As the original author of the code, you can expect that your contribution will be released and licensed under Apache License, v. 2.0. Additionally, certain contributions valuable to the broader PostgreSQL community might be released under the PostgreSQL license. If your patch is beneficial for upstream PostgreSQL, we can offer it for review individually or include it with a set of changes.

If you are NOT the author of the code you are contributing to Greengage, please make sure you take proper licensing into account. Check the third-party license terms for similarity to the Apache License 2.0. Similar licenses are listed on the Apache Software Foundation website under [Category A](https://www.apache.org/legal/resolved.html#category-a). Note that some of these licenses require making proper attribution in the [NOTICE file](https://github.com/GreengageDB/greengage/blob/main/NOTICE) (see examples [here](https://github.com/GreengageDB/greengage/blob/main/NOTICE#L335)).

Do NOT remove licensing headers from any piece of work done by a third party. Even partial usage of someone else’s work may assume licensing implications. Please give the original author credit for their work by keeping the licensing headers

## Coding guidelines

Before introducing a major change, it is always good to validate your idea with the architectural committee. [Create an issue on GitHub](https://github.com/GreengageDB/greengage/issues) and explain what’s on your mind before you spend hours writing code. We expect that while explaining your proposal to the committee, you’ll be specific about the approaches you are going to use and reasons behind using them.

Submitting changes in small portions is the best strategy, even if you are working on a massive feature. Smaller patches can be reviewed within a week while large changesets require more time for being checked by the committee members. To get timely feedback and see your code merged into the project faster, stick to small, granular pull requests. This is also a way to show the reviewers that their job is valued and respected.

To help you with the process of coding and describing your pull requests for reviewers, we have created a separate [Pull Request Submission Guidelines](https://greengagedb.org/en/blog/contributing.html) document. Please refer to it when in doubt and contact us if the document provides no answer.

Here we mention just a few best practices we expect you to apply while contributing to Greengage DB. For detailed recommendations, please refer to Greengage DB’s [Pull Request Submission Guidelines](https://greengagedb.org/en/blog/contributing.html).

 - Follow [PostgreSQL Coding Conventions](https://www.postgresql.org/docs/devel/source.html) when writing C/C++ code for Greengage.
 - Run **pgindent** for C and Perl code as per [README.gpdb](https://github.com/GreengageDB/greengage/blob/main/src/tools/pgindent/README.gpdb).
 - Use [Pylint](https://www.pylint.org/) for all Python code.
 - Format all Golang code in accordance with [gofmt](https://golang.org/cmd/gofmt/).

Use git `diff --color` as you review your changes to avoid spurious whitespace issues in the submitted code.

Regression tests are mandatory for every new feature that you contribute to Greengage. All tests covering new functionality should also be contributed to the project. Check [Pull Request Submission Guidelines](https://greengagedb.org/en/blog/contributing.html) to make sure that all tests are placed in the right folders within the project repository. If you need guidance related to testing or documenting your contributions, please explicitly include your questions in the pull request, and the architectural committee members will address them during patch review.

At the very minimum you should always make sure that all local test runs are successful before submitting a pull request (PR) to the main repository.

## PostgreSQL-related changes

We prefer to get the changes related to the shared functionality of PostgreSQL and Greengage DB reviewed by the members of both communities. The larger Postgres community has more resources to help improve your patches, that’s why we may request submitting your PostgreSQL-related changes to Postgres upstream to leverage that power and reduce the delta between Greengage DB and PostgreSQL. If your contribution is likely to be forward-ported to PostgreSQL, please refer to PostgreSQL code base where appropriate.

## Patch submission

We expect that in the repository you forked from the Greengage DB one you’ll create a branch **other than main** that will contain the changes you prepared to share with us and the rest of the community. Then [send us a pull request](https://help.github.com/articles/about-pull-requests/).

Do keep in mind that your patch review will slow down
 - if you do not follow [Pull Request Submission Guidelines](https://greengagedb.org/en/blog/contributing.html)
 - if tests are missing or copied to incorrect folders

## Patch review

All submitted patches are subject to review by the architectural committee members. The time required for review depends of the volume and complexity of the submitted patch:
 - Up to 1 week for small/easy patches;
 - Up to 4 weeks for patches of medium complexity
 - Up to 8 weeks for patches of extra size or complexity.

Architectural committee reserves the right to decline patches without review if they introduce no valuable changes and/or contain garbage code.

Each contributed patch should get approvals from two architectural committee members before being merged into the project. Both pull request description and code will be thoroughly examined to ensure high quality and security.

The first reviewer might initiate a discussion proposing further improvements / changes to your pull request. You might also be asked to explain certain solutions and approaches applied as part of the proposed changeset. Discussions around pull requests along with any other communication in the Greengage DB project are covered by the Code of Conduct. Once the patch is accepted by the first reviewer, the second reviewer steps up to double-check your contribution and possibly provide a final commentary.

Note that the members of committee volunteer to review patches. Therefore, their availability may be limited, and reasonable delays are possible. In many cases, being proactive and asking for feedback may speed up the review process.

After basic review (including feature relevancy, absence of malicious changes, etc.) and approve to run CI pipelines, the processes of patch review begins.

## Validation checks and CI

Your patch will undergo a series of validation checks from our automated CI pipeline. If any of them fails, you will need to change the patch you contributed so as to pass this check next time.

While the process is generally intuitive and enables you understand what exactly was wrong, do not hesitate to ask your reviewers for help if you don’t know why your approved patch was not merged. Use the pull request discussion to contact them.

## Direct commits to the repository

Members of the architectural committee may sometimes commit to the repository directly, without submitting pull requests. Usually they do so to introduce minor changes (i.e. typo corrections), all major code contributions need to be submitted as pull requests and go through checks.
