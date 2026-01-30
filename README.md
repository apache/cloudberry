<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Cloudberry (Incubating)

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="./.github/full_color_black.svg">
    <source media="(prefers-color-scheme: dark)" srcset="./.github/full_color_white.svg">
    <img alt="Apache Cloudberry Logo" src="./.github/full_color_black.svg" width="400px">
  </picture>
</p>

[![Website](https://img.shields.io/badge/Website-eebc46)](https://cloudberry.apache.org)
[![Documentation](https://img.shields.io/badge/Documentation-acd94a)](https://cloudberry.apache.org/docs)
[![Slack](https://img.shields.io/badge/Join_Slack-6a32c9)](https://inviter.co/apache-cloudberry)
[![Twitter Follow](https://img.shields.io/twitter/follow/ASFCloudberry)](https://twitter.com/ASFCloudberry)
[![WeChat](https://img.shields.io/badge/WeChat-eebc46)](https://cloudberry.apache.org/community/wechat)
[![Youtube](https://img.shields.io/badge/Youtube-gebc46)](https://youtube.com/@ApacheCloudberry)
[![GitHub Discussions](https://img.shields.io/github/discussions/apache/cloudberry)](https://github.com/apache/cloudberry/discussions)
![GitHub commit activity(branch)](https://img.shields.io/github/commit-activity/m/apache/cloudberry)
![GitHub contributors](https://img.shields.io/github/contributors/apache/cloudberry)
![GitHub License](https://img.shields.io/github/license/apache/cloudberry)
[![Apache Cloudberry Build](https://github.com/apache/cloudberry/actions/workflows/build-cloudberry.yml/badge.svg)](https://github.com/apache/cloudberry/actions/workflows/build-cloudberry.yml)
<a href="https://scan.coverity.com/projects/apache-cloudberry-1f6d497c-9dcb-4204-a37b-0d79c6c5bec3">
  <img alt="Coverity Scan Build Status"
       src="https://scan.coverity.com/projects/31473/badge.svg"/>
</a>
<a href="https://sonarcloud.io/summary/new_code?id=apache_cloudberry">
  <img alt="SonarQube Cloud" src="https://sonarcloud.io/images/project_badges/sonarcloud-highlight.svg" width="100px">
</a>
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/apache/cloudberry)
[![Apache Rat Audit](https://github.com/apache/cloudberry/actions/workflows/apache-rat-audit.yml/badge.svg)](https://github.com/apache/cloudberry/actions/workflows/apache-rat-audit.yml)
---------

## Introduction

Apache Cloudberry (Incubating), created by the original developers of
Greenplum Database, is one advanced and mature open-source Massively Parallel
Processing (MPP) database, which evolves from the open-source version of the
Pivotal Greenplum Database®️ but features a newer PostgreSQL kernel and more
advanced enterprise capabilities. It can serve as a data warehouse and can
also be used for large-scale analytics and AI/ML workloads.

## Build and try out

### Build from source

You can follow [these guides](https://cloudberry.apache.org/docs/deployment/) to build Cloudberry on
Linux OS (including RHEL/Rocky Linux, and Ubuntu) and macOS.

### Try out quickly

Welcome to try out Cloudberry via building [one Docker-based
Sandbox](./devops/sandbox), which is tailored to help you gain a basic
understanding of Cloudberry's capabilities and features.

## Repositories

This is the main repository for Apache Cloudberry (Incubating). Alongside
this, there are several ecosystem repositories for Cloudberry, including the
website, extensions, connectors, adapters, and other utilities.

* [apache/cloudberry-site](https://github.com/apache/cloudberry-site): website and documentation sources.
* [apache/cloudberry-backup](https://github.com/apache/cloudberry-backup): backup utility for Cloudberry.
* [apache/cloudberry-go-libs](https://github.com/apache/cloudberry-go-libs): go-libs for Cloudberry.
* [apache/cloudberry-pxf](https://github.com/apache/cloudberry-pxf): Platform Extension Framework (PXF) for Cloudberry.

## Community & Support

We have many channels for community members to discuss, ask for help,
feedback, and chat:

| Type | Description |
|------|-------------|
| Slack | [Click to Join](https://inviter.co/apache-cloudberry) the real-time chat on Slack for QA, Dev, Events, and more. Don't miss out! Check out the [Slack guide](https://cloudberry.apache.org/community/slack) to learn more. |
| Q&A | Ask for help when running/developing Cloudberry, visit [GitHub Discussions - QA](https://github.com/apache/cloudberry/discussions/categories/q-a). |
| New ideas / Feature Requests | Share ideas for new features, visit [GitHub Discussions - Ideas](https://github.com/apache/cloudberry/discussions/categories/ideas-feature-requests).  |
| Report bugs | Problems and issues in Apache Cloudberry core. If you find bugs, welcome to submit them [here](https://github.com/apache/cloudberry/issues).  |
| Report a security vulnerability | View our [security policy](https://github.com/apache/cloudberry/security/policy) to learn how to report and contact us.  |
| Community events | Including meetups, webinars, conferences, and more events, visit the [Events page](https://cloudberry.apache.org/community/events) and subscribe events calendar.  |
| Documentation | [Official documentation](https://cloudberry.apache.org/docs/) for Cloudberry. You can explore it to discover more details about us. |

## Contribution

Contributions can be diverse, such as code enhancements, bug fixes, feature
proposals, documents, marketing, and so on. No contribution is too small, we
encourage all types of contributions. Cloudberry community welcomes
contributions from anyone, new and experienced! Our [contribution
guide](https://cloudberry.apache.org/contribute) will help you get started
with the contribution.

| Type | Description |
|----|---------------|
| Code contribution | Learn how to contribute code to the Cloudberry, including coding preparation, conventions, workflow, review, and checklist following the [code contribution guide](https://cloudberry.apache.org/contribute/code).|
| Submit the proposal | Proposing major changes to Cloudberry through [proposal guide](https://cloudberry.apache.org/contribute/proposal).|
| Doc contribution | We need you to join us to help us improve the documentation, see the [doc contribution guide](https://cloudberry.apache.org/contribute/doc).|

## Roadmap

You can check our [Cloudberry Roadmap
](https://github.com/apache/cloudberry/discussions/868) out to see the product
plans and goals we want to achieve. Welcome to share your thoughts and ideas
to join us in shaping the future of Apache Cloudberry (Incubating). (We will
update the Roadmap after entering the Incubator.)

## Acknowledgment

Thanks to [PostgreSQL](https://www.postgresql.org/), [Greenplum
Database](https://greenplum.org/) and other great open source projects to make
Apache Cloudberry has a sound foundation.

## License

Cloudberry is licensed under the Apache License, Version 2.0. For details, see
the [LICENSE](./LICENSE).

## ASF Incubator disclaimer

Apache Cloudberry is an effort undergoing incubation at The Apache Software
Foundation (ASF), sponsored by the Apache Incubator. Incubation is required
for all newly accepted projects until a further review indicates that the
infrastructure, communications, and decision making process have stabilized in
a manner consistent with other successful ASF projects. While incubation
status is not necessarily a reflection of the completeness or stability of the
code, it does indicate that the project has yet to be fully endorsed by the
ASF.
