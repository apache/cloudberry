<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="./.github/full_color_black.svg">
    <source media="(prefers-color-scheme: dark)" srcset="./.github/full_color_white.svg">
    <img alt="Cloudberry Database Logo" src="./.github/full_color_black.svg" width="400px">
  </picture>
</p>

<p align="center">
    Next Generation Unified Database for Analytics and AI
</p>

[![Website](https://img.shields.io/badge/Website-eebc46)](https://cloudberrydb.org)
[![Documentation](https://img.shields.io/badge/Documentation-acd94a)](https://cloudberrydb.org/docs)
[![Slack](https://img.shields.io/badge/Join_Slack-6a32c9)](https://communityinviter.com/apps/cloudberrydb/welcome)
[![Twitter Follow](https://img.shields.io/twitter/follow/cloudberrydb)](https://twitter.com/cloudberrydb)
[![WeChat](https://img.shields.io/badge/WeChat-eebc46)](https://cloudberrydb.org/community/wechat)
[![Youtube](https://img.shields.io/badge/Youtube-gebc46)](https://youtube.com/@cloudberrydb)
[![GitHub Discussions](https://img.shields.io/github/discussions/cloudberrydb/cloudberrydb)](https://github.com/apache/cloudberrydb/discussions)
![GitHub commit activity(branch)](https://img.shields.io/github/commit-activity/m/cloudberrydb/cloudberrydb)
![GitHub contributors](https://img.shields.io/github/contributors/cloudberrydb/cloudberrydb)
![GitHub License](https://img.shields.io/github/license/cloudberrydb/cloudberrydb)
![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fcloudberrydb%2Fcloudberrydb.svg?type=shield)
[![cbdb pipeline](https://github.com/cloudberrydb/cloudberrydb/actions/workflows/build.yml/badge.svg)](https://github.com/cloudberrydb/cloudberrydb/actions/workflows/build.yml)

---------

## What's Cloudberry Database

Cloudberry Database (`CBDB` or `CloudberryDB` for short) is created by a bunch
of original Greenplum Database developers and ASF committers. We aim to bring
modern computing capabilities to the traditional distributed MPP database to
support Analytics and AI/ML workloads in one platform.

As a derivative of Greenplum Database 7, Cloudberry Database is compatible
with Greenplum Database, but it's shipped with a newer PostgreSQL 14.4 kernel
(scheduled kernel upgrade yearly) and a bunch of features Greenplum Database
lacks or does not support. View the [Cloudberry Database vs Greenplum
Database](https://cloudberrydb.org/docs/cbdb-vs-gp-features) doc for details.

## Roadmap

You can check our [Cloudberry Database Roadmap
2024](https://github.com/apache/cloudberrydb/discussions/369) out to see the
product plans and goals we want to achieve in 2024. Welcome to share your
thoughts and ideas to join us in shaping the future of the Cloudberry
Database.

## Build and try out

### Build from source

You can follow [these guides](./deploy/build) to build the Cloudberry Database on
Linux OS(including CentOS, RHEL/Rocky Linux, and Ubuntu) and macOS.

### Try out quickly

Welcome to try out Cloudberry Database via building [one Docker-based
Sandbox](https://github.com/cloudberrydb/bootcamp), which is tailored to help
you gain a basic understanding of Cloudberry Database's capabilities and
features a range of materials, including tutorials, sample code, and crash
courses.

## Repositories

This is the main repository for Cloudberry Database. Alongside this, there are
several ecosystem repositories for the Cloudberry Database, including the
website, extensions, connectors, adapters, and other utilities.

* [cloudberrydb/cloudberrydb-site](https://github.com/cloudberrydb/cloudberrydb-site): website and documentation sources.
* [cloudberrydb/bootcamp](https://github.com/cloudberrydb/bootcamp): help you quickly try out Cloudberry Database via one Docker-based Sandbox.
* [cloudberrydb/gpbackup](https://github.com/cloudberrydb/gpbackup): backup utility for Cloudberry Database.
* [cloudberrydb/gp-common-go-libs](https://github.com/cloudberrydb/gp-common-go-libs): gp-common-go-libs for Cloudberry Database.
* [cloudberrydb/gpbackup-s3-plugin](https://github.com/cloudberrydb/gpbackup-s3-plugin): S3 plugin for use with Cloudberry Database backup utility. 
* [cloudberrydb/filedump](https://github.com/cloudberrydb/filedump): format heap/index/control files into a human-readable form.
* [cloudberrydb/postgis](https://github.com/cloudberrydb/postgis): PostGIS for Cloudberry Database.
* [cloudberrydb/pxf](https://github.com/cloudberrydb/pxf): Platform Extension Framework (PXF) for Cloudberry Database.
* [cloudberrydb/madlib](https://github.com/cloudberrydb/madlib): MADlib® for Cloudberry Database.
* [cloudberrydb/plr](https://github.com/cloudberrydb/plr): PL/R for Cloudberry Database. 
* [cloudberrydb/pljava](https://github.com/cloudberrydb/pljava): PL/Java for Cloudberry Database.
* More is coming...

## Community & Support

We have many channels for community members to discuss, ask for help,
feedback, and chat:

| Type | Description |
|------|-------------|
| Slack | [Click to Join](https://communityinviter.com/apps/cloudberrydb/welcome) the real-time chat on Slack for QA, Dev, Events, and more. Don't miss out! Check out the [Slack guide](https://cloudberrydb.org/community/slack) to learn more. |
| Q&A | Ask for help when running/developing Cloudberry Database, visit [GitHub Discussions - QA](https://github.com/apache/cloudberrydb/discussions/categories/q-a). |
| New ideas / Feature Requests | Share ideas for new features, visit [GitHub Discussions - Ideas](https://github.com/apache/cloudberrydb/discussions/categories/ideas-feature-requests).  |
| Report bugs | Problems and issues in Cloudberry Database core. If you find bugs, welcome to submit them [here](https://github.com/cloudberrydb/cloudberrydb/issues).  |
| Report a security vulnerability | View our [security policy](https://github.com/cloudberrydb/cloudberrydb/security/policy) to learn how to report and contact us.  |
| Community events | Including meetups, webinars, conferences, and more events, visit the [Events page](https://cloudberrydb.org/community/events) and subscribe events calendar.  |
| Documentation | [Official documentation](https://cloudberrydb.org/docs/) for Cloudberry Database. You can explore it to discover more details about us. |

When you are involved, please follow our community [Code of
Conduct](https://cloudberrydb.org/community/coc) to help create a safe space
for everyone.

## Contribution

We believe in the Apache Way "Community Over Code" and we want to make
Cloudberry Database a community-driven project.

Contributions can be diverse, such as code enhancements, bug fixes, feature
proposals, documents, marketing, and so on. No contribution is too small, we
encourage all types of contributions. Cloudberry Database community welcomes
contributions from anyone, new and experienced! Our [contribution
guide](https://cloudberrydb.org/contribute) will help you get started with the
contribution.

| Type | Description |
|----|---------------|
| Code contribution | Learn how to contribute code to the Cloudberry Database, including coding preparation, conventions, workflow, review, and checklist following the [code contribution guide](https://cloudberrydb.org/contribute/code).|
| Submit the proposal | Proposing major changes to Cloudberry Database through [proposal guide](https://cloudberrydb.org/contribute/proposal).|
| Doc contribution | We need you to join us to help us improve the documentation, see the [doc contribution guide](https://cloudberrydb.org/contribute/doc).|

## Contributors Wall

Thanks to all the people who already contributed!

<a href="https://github.com/cloudberrydb/cloudberrydb/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=cloudberrydb/cloudberrydb&max=800&columns=20&anon=0" />
</a>

<sub>Please note that the images shown above highlight the avatars of our
active and upstream contributors while not including anonymous
contributors. To view all the contributors, you can click on the images.</sub>

## Acknowledgment

Thanks to [PostgreSQL](https://www.postgresql.org/), [Greenplum
Database](https://greenplum.org/) and other great open source projects to make
Cloudberry Database has a sound foundation.

## License

Cloudberry Database is released under the [Apache License, Version
2.0](https://github.com/cloudberrydb/cloudberrydb/blob/main/LICENSE).
