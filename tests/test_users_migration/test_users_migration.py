import pytest


@pytest.fixture()
def start_main_class():
    start = users_migration.Users()
    return start


def test_one(start_main_class):
    print(start_main_class.handler())
