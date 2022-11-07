import typing
import dagster
from .mapmatch import mapmatch_bus_data
from .extract import icts_data
from .network import load_network
from odmkraken.resources import RESOURCES


@dagster.repository
def busspeeds():
    return [
        mapmatch_bus_data.to_job(resource_defs=RESOURCES),
        icts_data.to_job(resource_defs=RESOURCES),
        load_network.to_job(resource_defs=RESOURCES)
    ]