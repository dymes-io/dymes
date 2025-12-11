<!--
SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors

SPDX-License-Identifier: Apache-2.0
-->

# Contributing to the Dymes project

## Code of Conduct ##

Please adhere to the Dymes [code of conduct](./CODE_OF_CONDUCT.md) when interacting with
others in the project.

## Strict No LLM / No AI Policy

No LLMs for issues.

No LLMs for pull requests.

No LLMs for comments on the bug tracker, including translation. English is encouraged, but not required. You are welcome to post in your native language and rely on others to have their own translation tools of choice to interpret your words.

## Reporting Bugs ##

We are using [GitHub Issues](https://github.com/dymes-io/dymes-oss/issues)
for our public bugs. We keep a close eye on them and try to make it
clear when we have an internal fix in progress. Before filing a new
task, try to make sure your problem doesn't already exist.

If you found a bug, please report it, as far as possible, with:

- a detailed explanation of steps to reproduce the error
- the browser and browser version used
- a dev tools console exception stack trace (if available)

If you found a bug which you think is better to discuss in private (for
example, security bugs), consider first sending an email to `dymes-io@protonmail.com`.

**We don't have a formal bug bounty program for security reports; this
is an open source application, and your contribution will be recognized
in the changelog.**

## Pull Requests ##

If you want to propose a change or bug fix via a pull request (PR),
you should first carefully read the section **Developer's Certificate of
Origin**. You must also format your code and commits according to the
instructions below.

If you intend to fix a bug, it's fine to submit a pull request right
away, but we still recommend filing an issue detailing what you're
fixing. This is helpful in case we don't accept that specific fix but
want to keep track of the issue.

If you want to implement or start working on a new feature, please
open a **question** / **discussion** issue for it. No PR
will be accepted without a prior discussion about the changes,
whether it is a new feature, an already planned one, or a quick win.

If it is your first PR, you can learn how to proceed from
[this free video
series](https://egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github)

We use the `easy fix` tag to indicate issues that are appropriate for beginners.

## Commit Guidelines ##

Please follow the [Commit Message Guidelines](./CONVENTIONS.md#commit-message-guidelines), with commit
messages following the structure:

```
    <type>(subsystem): <summary>
    [optional body]
```


## Developer's Certificate of Origin (DCO)

By submitting code you agree to and can certify the following:

    Developer's Certificate of Origin 1.1

    By making a contribution to this project, I certify that:

    (a) The contribution was created in whole or in part by me and I
        have the right to submit it under the open source license
        indicated in the file; or

    (b) The contribution is based upon previous work that, to the best
        of my knowledge, is covered under an appropriate open source
        license and I have the right under that license to submit that
        work with modifications, whether created in whole or in part
        by me, under the same open source license (unless I am
        permitted to submit under a different license), as indicated
        in the file; or

    (c) The contribution was provided directly to me by some other
        person who certified (a), (b) or (c) and I have not modified
        it.

    (d) I understand and agree that this project and the contribution
        are public and that a record of the contribution (including all
        personal information I submit with it, including my sign-off) is
        maintained indefinitely and may be redistributed consistent with
        this project or the open source license(s) involved.

Then, all your code patches (**documentation is excluded**) should
contain a sign-off at the end of the patch/commit description body. It
can be automatically added by adding the `-s` parameter to `git commit`.

This is an example of what the line should look like:

```
Signed-off-by: Joe Bloggs <joe.bloggs@somewhere.com>
```

Please use your real name (sorry, no pseudonyms or anonymous
contributions are allowed).
