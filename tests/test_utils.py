import os
import pdb

# for importing the self-written Python modules, change working dir
if os.path.basename(os.getcwd()) == 'tests':
    os.chdir('..')
from utils import find_optimal_number_of_AZs, handle_exceptions


TEST_CASES_FIND_NUM_AZS = [
    {
        'num_subnets' : 2,
        'num_AZs' : 2,
        'expected_result_num_AZs' : 2
    },
    {
        'num_subnets' : 10,
        'num_AZs' : 2,
        'expected_result_num_AZs' : 2
    },
    {
        'num_subnets' : 4,
        'num_AZs' : 10,
        'expected_result_num_AZs' : 2
    },
    {
        'num_subnets' : 6,
        'num_AZs' : 10,
        'expected_result_num_AZs' : 3
    },
    {
        'num_subnets' : 1,
        'num_AZs' : 10,
        'expected_result_num_AZs' : 1
    },
    {
        'num_subnets' : 10,
        'num_AZs' : 10,
        'expected_result_num_AZs' : 5
    },
    {
        'num_subnets' : 3,
        'num_AZs' : 10,
        'expected_result_num_AZs' : 2
    },
    {
        'num_subnets' : 13,
        'num_AZs' : 10,
        'expected_result_num_AZs' : 6
    },
]


def test_find_optimal_number_of_AZs():
    """
    Tests the function for determining the optimal number of AZs.
    """
    for case in TEST_CASES_FIND_NUM_AZS:
        result =  find_optimal_number_of_AZs(case['num_subnets'], case['num_AZs'])
        assert result == case['expected_result_num_AZs']


def test_handle_exceptions():
    """
    Tests decorator for handling exceptions.
    """
    # check if exception gets handled
    @handle_exceptions('mock_function', 'test Exception')
    def mock_function():
        raise Exception
    mock_function()
