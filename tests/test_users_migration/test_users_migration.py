import pytest
from actions.migration.user import migration


@pytest.fixture()
def start_main_class():
    start = users_migration.Users()
    return start


def test_one(start_main_class):
    print(start_main_class.handler())
