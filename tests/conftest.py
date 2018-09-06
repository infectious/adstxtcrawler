import pytest


# Shamelessly copied from the pytest docs.
# https://docs.pytest.org/en/latest/example/simple.html

def pytest_addoption(parser):
    parser.addoption("--integration", action="store_true",
                     default=False, help="run slow tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--integration"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_integration = pytest.mark.skip(
        reason="need --integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
