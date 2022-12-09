import pytest
from event.actions import migration


@pytest.fixture()
def start_main_class():
    start = migration.Migration()
    return start


def test_one(start_main_class):
    print(start_main_class.execute())
