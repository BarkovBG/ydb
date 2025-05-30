import pytest
import yatest

from pure import run_test, DATA_PATH
from test_utils import pytest_generate_tests_for_part
from yql_utils import pytest_get_current_part


def pytest_generate_tests(metafunc):
    current_part, part_count = pytest_get_current_part(yatest.common.source_path(__file__))
    return pytest_generate_tests_for_part(metafunc, current_part, part_count, DATA_PATH, template='.yqls')


@pytest.mark.parametrize('what', ['Results', 'Debug', 'RunOnOpt', 'LLVM'])
def test(suite, case, cfg, tmpdir, what, yql_http_file_server):
    return run_test(suite, case, cfg, tmpdir, what, yql_http_file_server)
