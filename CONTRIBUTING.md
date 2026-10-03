# Contributing to LetsGossip

First off, thank you for considering contributing to LetsGossip! It's people like you that make this tool great.

## Branch Naming Convention

To keep our repository organized and easy to track, we require a specific naming convention for all new branches. 

When you start working on a new feature or bug fix, please create your branch using the following format:
`[username]-[feature-or-bug-name]`

**Examples:**
- `dheeraj-6904-add-jwt-auth`
- `dheeraj-6904-fix-lease-timeout`

## Development Workflow

1.  **Clone the repository** locally.
2.  **Create your branch** following the naming convention above.
3.  **Make your changes**. Ensure your code follows the existing style and architecture.
4.  **Run tests locally** to verify nothing is broken:
    ```bash
    dotnet test DistributedJobScheduler.sln
    ```
5.  **Commit your changes** with a clear and descriptive commit message.
6.  **Push to your branch** and open a **Pull Request (PR)** against the `main` branch.

## Pull Request Guidelines

-   Ensure your PR title is descriptive (e.g., "Fix: resolve lease timeout bug in Postgres").
-   If your PR resolves an open issue, link it in the description using keywords like `Fixes #1` or `Resolves #2`.
-   All PRs require approval from code owners and must pass all CI checks before they can be squashed and merged.

Thank you for contributing!
