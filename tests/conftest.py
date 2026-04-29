import pytest
from lib.utils import get_spark_session

@pytest.fixture
def spark():
    return get_spark_session("LOCAL")