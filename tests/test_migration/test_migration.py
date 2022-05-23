import pytest
from actions.event import Main


@pytest.fixture()
def start_main_class():
    start = Main()
    return start


def test_one(start_main_class):
    print(start_main_class.execute())
