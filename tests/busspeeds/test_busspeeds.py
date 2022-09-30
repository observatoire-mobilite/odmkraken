import pytest
from odmkraken.busspeeds.repo import *


def test_resources():
    assert RESOURCES['local_postgres']
    assert RESOURCES['edmo_vehdata']
    assert RESOURCES['shortest_path_engine']


def test_initialize():
    rd = busspeeds()
    assert rd.has_job('mapmatch_bus_data')
