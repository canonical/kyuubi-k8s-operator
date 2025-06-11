#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--inplace-upgrades-multi-units",
        action="store_true",
        default=False,
        help="Deploy multiple kyuubi units, with ZK",
    )
    parser.addoption(
        "--inplace-upgrades-tls",
        action="store_true",
        default=False,
        help="Setup TLS",
    )
    parser.addoption(
        "--inplace-upgrades-metastore",
        action="store_true",
        default=False,
        help="Setup metastore",
    )
    parser.addoption(
        "--inplace-upgrades-image",
        action="store_true",
        default=False,
        help="Upgrade kyuubi OCI resource in addition to the charm",
    )


@pytest.fixture(scope="module")
def with_multi_units(request) -> bool:
    return request.config.getoption("--inplace-upgrades-multi-units")


@pytest.fixture(scope="module")
def with_tls(request) -> bool:
    return request.config.getoption("--inplace-upgrades-tls")


@pytest.fixture(scope="module")
def with_metastore(request) -> bool:
    return request.config.getoption("--inplace-upgrades-metastore")


@pytest.fixture(scope="module")
def with_image_upgrade(request) -> bool:
    return request.config.getoption("--inplace-upgrades-image")


@pytest.fixture(scope="module")
def skipif_no_metastore(with_metastore: bool) -> None:
    if not with_metastore:
        pytest.skip("No metastore available")
