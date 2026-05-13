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

# Guidelines for AI-assisted Contributions

Apache Cloudberry follows the ASF Generative Tooling Guidance
for the use of AI-assisted development tools:

https://www.apache.org/legal/generative-tooling.html

This document provides additional project-specific guidance and
best practices for using AI tools in the Cloudberry community.
It is intended to supplement ASF guidance, not replace it.

## Guidelines

### 1. You own the code

AI-generated code carries the same responsibility as code you
type yourself. Review it before submitting. If a bug ships,
"the AI wrote it" is not a defense.

**Example:** As an experiment, you used an LLM to generate a
new type of executor node. The results were impressive, and you
wanted to share them with the community. Before opening a PR,
read every line, verify the logic, and make sure it fits with
existing code patterns. Someone might use your code in
production, not just for experiments.

### 2. Same quality bar

AI-assisted contributions must pass the same review, testing,
and CI standards as any other code. No shortcuts. AI-generated
code must come with corresponding tests, or be covered by
existing ones. If the AI wrote the code, you should at least
write or carefully verify the tests.

**Example:** You use an LLM to implement a new aggregate
function. The PR must include regression tests in `src/test`
that exercise both normal and edge cases.

### 3. Watch the license

Don't let AI introduce code incompatible with the Apache
License 2.0. You are responsible for ensuring all submitted
code — AI-generated or not — has proper licensing.

See [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html)
for details.

**Example:** If an AI tool reproduces a snippet from a
GPL-licensed project, you must not include it. When in doubt,
rewrite from scratch.

### 4. Flag it

When your PR includes significant AI-generated code, check the
AI disclosure box in the PR template. You don't have to
disclose minor AI assistance (autocomplete, reformatting), but
be transparent about substantial generation.

You can also record AI assistance directly in the commit
message using the optional `Assisted-by:` trailer (one line
per tool), following the same convention used by the Linux
kernel. See the
[Linux kernel coding assistants guidance](https://docs.kernel.org/process/coding-assistants.html)
for background.

```
Assisted-by: ChatGPT
Assisted-by: GitHub Copilot
```

This trailer is optional and non-binding — it provides
lightweight provenance information without making AI
disclosure a strict requirement.

**Example:** Using an LLM to autocomplete a single function
signature — no need for a flag. Using an LLM to generate an
entire new GUC parameter with validation logic — flag it and
add an `Assisted-by:` trailer. The flag doesn't mean the PR
skips review or merge criteria, but it gives reviewers more
context about the generation method and lets them focus on
architecture and logic rather than specific operators.

### 5. No meaningless code refactoring

Our core is PostgreSQL, and refactoring work has already been
done here. Rewriting code significantly complicates rebase.
Also, refactoring changes the code in a way that forces people
to relearn code they already know. Keep changes as simple as
possible.

**Example:** LLMs are eager to refactor. One day you may be
asked: "This code is not very good. Do you want to improve
it?" Of course! It could happen several times. Tokens are
spent, but what is the point of such refactoring?
(Rhetorical question)

### 6. LLM code review

Some AI review tools (such as GitHub Copilot Review or
CodeRabbit) may not currently be available for ASF-hosted
repositories due to operational, budgetary, or permission
reasons. Contributors can still use personal AI tools locally
but are responsible for ensuring code quality, compliance with
licensing terms, and reviewing outcomes.

**Example:** One could use GitHub Copilot for automated AI
code review on pull requests. Here are some important points:

- Copilot suggestions are **non-binding hints**, not
  requirements.
- If a suggestion is irrelevant or wrong, skip it — you know
  your code best.
- If a suggestion catches a real issue, fix it like you would
  for any review comment.
- Copilot does not replace human reviewers. All PRs still need
  approval from a committer.

### 7. Talk to maintainers yourself

Review discussions should reflect the contributor's own
understanding and technical judgment. AI tools may assist with
drafting responses, but contributors should engage
thoughtfully and personally with reviewers. Maintainers invest
time reviewing your code; respond in kind.

**Example:** A reviewer asks "why did you choose this approach
over X?" — write your own answer explaining the tradeoff,
don't paste an LLM-generated reply.

## AGENTS.md

[AGENTS.md](https://agents.md/) is a README for agents: a
dedicated, predictable place to provide context and
instructions to help AI coding agents work on your project.
We do not ship a repository-level `AGENTS.md` because the
right content is platform- and user-specific. If you work with
AI coding agents locally, create your own `AGENTS.md`. You could
take the template from the `AGENTS.md.template` file.

## Good uses of AI

- Bug fixing and root cause analysis
- Code review
- Writing and improving tests
- Documentation and code comments
- Build system and CI improvements
- Security research and vulnerability scanning
- Learning the codebase faster

## Resources

- [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html)
  — Official Apache guidance on AI tool usage
- [GitHub Copilot](https://github.com/features/copilot)
  — AI pair programmer and code reviewer
- [CodeRabbit](https://www.coderabbit.ai/)
  — Yet another AI pair programmer and code reviewer
- [AGENTS.md](https://agents.md/)
  - README for agents