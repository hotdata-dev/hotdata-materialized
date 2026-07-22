# Releasing

Every release uses `./scripts/release.sh`. Do not bump versions, tag, or create GitHub Releases manually.

## One-time setup

- Install [GitHub CLI](https://cli.github.com/) (`gh`) and authenticate.
- Ensure PyPI [trusted publishing](https://docs.pypi.org/trusted-publishers/) is configured for this repo (`publish.yml` uses the `pypi` GitHub environment).

## Release steps

1. Add user-facing notes under `## [Unreleased]` in `CHANGELOG.md`.
2. Prepare the release PR:

   ```bash
   ./scripts/release.sh prepare patch   # or minor | major | X.Y.Z
   ```

   This bumps the version in `pyproject.toml` and
   `hotdata_materialized/__init__.py`, rolls the changelog, and opens the PR.

3. Merge the PR after CI passes.
4. Publish from a clean `main` checkout:

   ```bash
   git checkout main
   git pull
   ./scripts/release.sh publish
   ```

## What happens automatically

Pushing a `vX.Y.Z` tag triggers two workflows:

| Workflow | Purpose |
|----------|---------|
| `publish.yml` | Build wheel/sdist, verify tag↔version, publish to PyPI via trusted publishing |
| `release.yml` | Create the GitHub Release with notes from `CHANGELOG.md` |

## Recover a missing GitHub Release

If PyPI publish succeeded but the GitHub Release workflow failed, rerun
`release.yml` from the Actions tab via `workflow_dispatch` with the existing
tag — no retagging needed.
