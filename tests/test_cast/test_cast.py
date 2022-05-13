import pytest
from main.validation import validation


@pytest.fixture()
def start_main_class():
    start = data_cast.Cast()
    return start


def test_casting_function(start_main_class):
    print(start_main_class.execute())
