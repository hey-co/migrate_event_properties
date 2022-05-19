import pytest
from main.users_migration import users_migration


@pytest.fixture()
def start_main_class():
    start = users_migration.Users()
    return start


def test_one(start_main_class):
    print(start_main_class.handler())
