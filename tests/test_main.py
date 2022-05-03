import pytest
from main import main


@pytest.fixture()
def start_handler():
    start = main.Handler()
    return start


def test_one(start_handler):
    print(start_handler)