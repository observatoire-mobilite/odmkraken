import pytest
from odmkraken.busspeeds.repo import *


def test_resources():
    assert RESOURCES_TEST['postgres_connection']
    assert RESOURCES_TEST['edmo_vehdata']
    assert RESOURCES_TEST['shortest_path_engine']


def test_initialize():
    rd = busspeeds_test()
    assert rd.has_job('mapmatch_bus_data')
