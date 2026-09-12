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

# GitHub Copilot Instructions for Commit Message Review

## Overview
When reviewing pull requests, please check commit messages against our project's commit message template and standards defined in `.gitmessage`.

## Commit Message Review Guidelines

### Title Module Requirements
- **Format**: Use imperative mood starting with uppercase letter
- **Length**: Preferably 50 characters or less, maximum 72 characters
- **Ending**: Do NOT end with a period
- **Prefixes**: Recommended prefixes (but not mandatory):
  - `Fix:` for bug fixes, typos, and issue resolution
  - `Feature:` for new feature implementations
  - `Enhancement:` for code optimizations and improvements
  - `Doc:` for documentation changes
  - Any imperative verb starting with uppercase letter is acceptable

### Body Module Requirements
- **Separation**: Must have a blank line between title and body
- **Content**: Ensure meaningful and substantial content. Ideally include:
  - **What**: What changes were made
  - **Why**: Why the changes were necessary
  - **How**: How the changes were implemented (when relevant)
- **Compatibility**: Describe any compatibility issues if present
- **Line Length**: Maximum 72 characters per line

### Trailers Module (Optional)
Check for proper formatting of optional trailers:
- `Reported-by: NAME` for bug reports from others
- `Co-authored-by: NAME <EMAIL>` for multiple authors
- `on-behalf-of: @ORG NAME@ORGANIZATION.COM` for organizational commits
- `See: Issue#id <URL>` for GitHub Issues references
- `See: Discussion#id <URL>` for GitHub Discussions references

## Review Actions

When reviewing commits in PRs:

1. **Validate Title Format**:
   - Check if title follows imperative mood with uppercase start
   - Verify title length (prefer ≤50 chars, accept ≤72 chars)
   - Confirm no trailing period
   - Note: Prefixes like "Fix:" are recommended but not mandatory

2. **Check Body Structure**:
   - Verify blank line separation
   - Validate line length (72 chars max)
   - Ensure meaningful content that explains the change substantively
   - Prefer inclusion of what/why/how when applicable

3. **Review Trailers**:
   - Validate trailer formatting if present
   - Check URL validity for references

4. **Provide Feedback**:
   - Suggest corrections for non-compliant messages
   - Offer specific examples of proper formatting
   - Highlight which part of the template is violated

## Example Good Commit Messages

```
Fix: resolve memory leak in query executor

The query executor was not properly releasing memory allocated
for intermediate results when processing complex joins. This
change adds proper cleanup in the error handling paths and
ensures all allocated buffers are freed.

Co-authored-by: Jane Doe <jane@example.com>
See: Issue#123 <https://github.com/apache/cloudberry/issues/123>
```

```
Feature: add support for parallel index creation

Implements parallel index creation to improve performance on
large tables. The feature uses multiple worker processes to
build index segments concurrently, reducing creation time
by up to 60% on multi-core systems.

See: Discussion#456 <https://github.com/apache/cloudberry/discussions/456>
```

## Common Issues to Flag

- Title exceeding 72 characters (warn if over 50 characters)
- Title ending with period
- Title not starting with uppercase letter
- Missing blank line between title and body
- Body lines exceeding 72 characters
- Malformed trailer syntax
- Vague or insufficient commit descriptions lacking substance
- Non-imperative mood in title

When suggesting improvements, always reference the specific section of our `.gitmessage` template that applies.