<!--
SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors

SPDX-License-Identifier: Apache-2.0
-->

# Dymes Conventions

<!-- TOC -->
* [Dymes Conventions](#dymes-conventions)
  * [Coding Conventions](#coding-conventions)
    * [Whitespace](#whitespace)
    * [Naming](#naming)
      * [General rules of thumb](#general-rules-of-thumb)
      * [Types, functions and variables](#types-functions-and-variables)
    * [Formatting and indentation](#formatting-and-indentation)
  * [Commit Message Guidelines](#commit-message-guidelines)
    * [Commit message type](#commit-message-type)
    * [Commit message component/subsystem](#commit-message-componentsubsystem)
    * [Commit message summary](#commit-message-summary)
    * [Commit message body](#commit-message-body)
<!-- TOC -->

## Coding Conventions

The Dymes coding conventions match those applied to the primary Zig codebase and standard library.

### Whitespace

- 4 space indentation
- Open braces on same line, unless you need to wrap
- If a list of things is longer than 3-4, put each item on its own line and exercise the ability to put an extra comma at the end

### Naming

#### General rules of thumb

- Avoid Redundancy in Names

- Avoid these words in type names:
  - Value
  - Data
  - Context
  - Manager
  - utils, misc, or somebody's initials

Everything is a value, all types are data, everything is context, all logic manages state. Nothing is communicated by using a word that applies to all types.

#### Types, functions and variables


Roughly speaking: 

`camelCaseFunctionName`, `TitleCaseTypeName`, `snake_case_variable_name`. 

More precisely:

- If `x` is a type then `x` should be *TitleCase*, unless it is a struct with 0 fields and is never meant to be instantiated, in which case it is considered to be a "namespace" and uses *snake_case*.
- If `x` is callable, and `x`'s return type is type, then `x` should be *TitleCase*.
- If `x` is otherwise callable, then `x` should be *camelCase*.
- Otherwise, `x` should be *snake_case*.

### Formatting and indentation

Formatting and indention are as per `zig fmt`.

## Commit Message Guidelines

The Dymes commit guidelines are largely based on the [Conventional Commits](https://www.conventionalcommits.org) conventions.

The commit message structure is as follows:

```
    <type>(component): <summary>
    [optional body]
```

### Commit message type

Commit type must be one of the following:

| Commit Type | Description                                                                     |
|-------------|---------------------------------------------------------------------------------|
| feat        | A new feature or enhancement                                                    |
| fix         | A bug fix                                                                       |
| doc         | A documentation-only change                                                     |
| cicd        | CI/CD or build configuration and/or scripts                                     |
| perf        | Performance enhancement                                                         |
| reno        | Renovations (refactoring), code changes that neither fix a bug or add a feature |
| chore       | Bumping dependencies, adapting to API changes                                   |
| test        | Adding or fixing tests                                                          |
| ops         | Ops-related, such as Terraform, k8s or helm changes                             |

### Commit message component/subsystem

This indicates which component or subsystem in particular was affected by
the change, otherwise a cross-cutting change is assumed.

Dymes components:

| Subsystem   | Description                                                                     |
|-------------|---------------------------------------------------------------------------------|
| common      | Common utilities and data structures                                            |
| msg         | Message and header definitions                                                  |
| msg_store   | Message store                                                                   |
| engine      | Engine                                                                          |
| daemons     | Daemons like replica node, metrics collector, etc                               |
| http        | HTTP support                                                                    |
| vsr         | Viewstamped Replication                                                         |
| tooling     | Dymes data exporter, importer, etc                                              |

You may also be more specific regarding component, for example, a fix in the VSR transport subsystem could
have a commit message starting like this:

```
fix(vsr.transport): Fixed buffer overallocation during...
```


### Commit message summary

Use the commit summary to provide a succinct description of the change.

### Commit message body

Explain the motivation for the change in the commit message body. 

This should explain _why_ you are making the change. 

You can include a comparison of the previous behavior with the new behavior in order to illustrate the impact of the change.


