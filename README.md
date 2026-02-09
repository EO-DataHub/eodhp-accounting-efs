# EO Data Hub Platform: eodhp-accounting-efs

This collects accounting events relating to EFS storage use by workspaces.

This takes samples of storage use periodically and emits a 'consumption rate' message. For example,
storage use of 100GB implies a rate of 100GB-seconds per second, where GB-seconds are the units for
the underlying storage item being sold.

## Development

### Prerequisites

Install [uv](https://docs.astral.sh/uv/getting-started/installation/).

### Getting started

```commandline
make setup
```

This will install dependencies via `uv sync` and set up `pre-commit` hooks.

### Building and testing

Available `make` targets:

- `make test`: run tests continuously (using `pytest-watcher`)
- `make testonce`: run tests once
- `make check`: run linting (`ruff`), type checking (`pyright`), and validate `pyproject.toml`
- `make format`: auto-fix lint issues and format code
- `make dockerbuild`: build a `latest` Docker image (use `make dockerbuild VERSION=1.2.3` for a release image)
- `make dockerpush`: push a Docker image (again, you can add `VERSION=1.2.3`)

### Managing dependencies

Dependencies are specified in `pyproject.toml`. To add or update dependencies:

```commandline
uv add <package>        # add a runtime dependency
uv add --dev <package>  # add a dev dependency
```

The `uv.lock` lockfile is committed to the repository.

## Releasing

Ensure that `make check` and `make testonce` pass before continuing.

Docker images are built and pushed automatically via GitHub Actions when tags are pushed.
To create a release:

- `git tag v1.2.3`
- `git push --tags`
