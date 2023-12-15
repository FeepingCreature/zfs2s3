#!/usr/bin/env bash
set -euxo pipefail
mypy zfs2s3
flake8 zfs2s3
pylint zfs2s3
black zfs2s3
isort zfs2s3
bandit -r zfs2s3
radon cc zfs2s3
