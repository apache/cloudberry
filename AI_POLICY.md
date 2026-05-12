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

# AI Policy

We welcome AI tools in Apache Cloudberry development — code assistants, LLMs, AI code review, and beyond. AI is a normal developer tool, like an IDE or a debugger. This document sets simple ground rules so everyone can use AI responsibly.

## Guidelines

### 1. You own the code

AI-generated code carries the same responsibility as code you type yourself. Review it before submitting. If a bug ships, "the AI wrote it" is not a defense.

**Example:** As an experiment, you used LLM to generate a new type of executor node. The results were impressive, and you wanted to share them with the community. Before opening PR, read every line, verify the logic, and make sure it fits with existing code patterns. Someone might use your code in production, not just for experiments.

### 2. Same quality bar

AI-assisted contributions must pass the same review, testing, and CI standards as any other code. No shortcuts. AI-generated code must come with corresponding tests, or be covered by existing ones. If the AI wrote the code, you should at least write or carefully verify the tests.

**Example:** You use an LLM to implement a new aggregate function. The PR must include regression tests in `src/test` that exercise both normal and edge cases.

### 3. Watch the license

Don't let AI introduce code incompatible with the Apache License 2.0. You are responsible for ensuring all submitted code — AI-generated or not — has proper licensing.

See [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html) for details.

**Example:** If an AI tool reproduces a snippet from a GPL-licensed project, you must not include it. When in doubt, rewrite from scratch.

### 4. Flag it

When your PR includes significant AI-generated code, check the AI disclosure box in the PR template. You don't have to disclose minor AI assistance (autocomplete, reformatting), but be transparent about substantial generation.

**Example:** Using LLM to autocomplete a single function signature - no need for a flag. Using LLMs to generate an entire new GUC parameter with validation logic - flag it. The flag doesn't mean that the PR doesn't need to be reviewed or merged, but it will give reviewers more information about the code generation method and allow them to focus more on checking the architecture and logic, rather than specific operators.

### 5. No meaningless code refactoring

Our core is PostgreSQL, and refactoring work has already been done here. Rewriting code significantly complicates rebase. Also, refactoring changes the code in a way that forces people to relearn the code they already know. Keep changes as simple as possible.

**Example:** The point of LLM is to spend your tokens. One day, you will be asked: "This code is not very good. Do you want to improve it?" Of course! It could happen several times. Tokens are spent, but what is the point of such refactoring? (Rhetorical question)

### 6. LLM code review

So far, it is not possible to use paid LLM models for code review in open source ASF projects. However, one could use personal licenses for LLMs to do the same. 

**Example:** One could use GitHub Copilot for automated AI code review on pull requests. Here are some important points:

- Copilot suggestions are **non-binding hints**, not requirements.
- If a suggestion is irrelevant or wrong, skip it — you know your code best.
- If a suggestion catches a real issue, fix it like you would for any review comment.
- Copilot does not replace human reviewers. All PRs still need approval from a committer.

### 7. Talk to maintainers yourself

Do not use AI to auto-generate responses to review feedback. Maintainers invest time reviewing your code; respond thoughtfully and personally.

**Example:** A reviewer asks "why did you choose this approach over X?" — write your own answer explaining the tradeoff, don't paste an LLM-generated reply.

## Good uses of AI

- Bug fixing and root cause analysis
- Code review
- Writing and improving tests
- Documentation and code comments
- Build system and CI improvements
- Security research and vulnerability scanning
- Learning the codebase faster

## Resources

- [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html) - Official Apache guidance on AI tool usage
- [GitHub Copilot](https://github.com/features/copilot) - AI pair programmer and code reviewer
- [LLM Leaderboard](https://llm-stats.com/) - LLM Stats Score, it's better to use high-ranked models
