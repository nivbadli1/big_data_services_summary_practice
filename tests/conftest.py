"""
pytest setup for the QuickBite capstone.

    pytest -q tests/                      # tests the SOLUTION files (default)
    pytest -q tests/ --target=student     # tests YOUR files (apps/consumer, airflow/dags)

Inside the apps container the repo folders are mounted at /apps, /airflow, /solution, /seed;
on a laptop they are relative to the repo root. Both layouts work.
"""
import importlib.util
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)


def _first_existing(*candidates):
    for c in candidates:
        if os.path.exists(c):
            return c
    return candidates[0]


def _paths(target):
    if target == "student":
        return {
            "consumer": _first_existing(os.path.join(REPO, "apps", "consumer", "consumer.py"),
                                        "/apps/consumer/consumer.py"),
            "transform": _first_existing(os.path.join(REPO, "airflow", "dags", "quickbite_transform.py"),
                                         "/airflow/dags/quickbite_transform.py"),
        }
    return {
        "consumer": _first_existing(os.path.join(REPO, "solution", "apps", "consumer", "consumer.py"),
                                    "/solution/apps/consumer/consumer.py"),
        "transform": _first_existing(os.path.join(REPO, "solution", "airflow", "dags", "quickbite_transform.py"),
                                     "/solution/airflow/dags/quickbite_transform.py"),
    }


def pytest_addoption(parser):
    parser.addoption("--target", action="store", default="solution", choices=["solution", "student"],
                     help="which implementation to test")


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def target(request):
    return request.config.getoption("--target")


@pytest.fixture(scope="session")
def consumer(target):
    return _load(f"consumer_{target}", _paths(target)["consumer"])


@pytest.fixture(scope="session")
def qt(target):
    return _load(f"quickbite_transform_{target}", _paths(target)["transform"])


@pytest.fixture(scope="session")
def seed_module():
    seed_dir = _first_existing(os.path.join(REPO, "seed"), "/seed")
    apps_dir = _first_existing(os.path.join(REPO, "apps"), "/apps")
    if apps_dir not in sys.path:
        sys.path.insert(0, apps_dir)
    return _load("seed_orders", os.path.join(seed_dir, "seed_orders.py"))


@pytest.fixture(scope="session")
def seed_docs(seed_module):
    return seed_module.build_docs()


def pytest_configure(config):
    # Registered here as well as in pytest.ini: inside the apps container the tests run from
    # /tests, where the repo-root pytest.ini is not visible.
    config.addinivalue_line("markers", "part5: needs the Part 5 chaos fixes")
    config.addinivalue_line("markers", "mongo: needs a running MongoDB (MONGO_URI)")
