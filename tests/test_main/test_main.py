import pytest
from main.Migration import migration


@pytest.fixture()
def start_main_class():
    start = main.Main()
    return start


def test_one(start_main_class):
    print(start_main_class.execute())