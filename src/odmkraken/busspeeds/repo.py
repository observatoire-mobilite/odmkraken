import typing
import dagster
from .mapmatch import mapmatch_bus_data, mapmatch_config
from .extract import normalized_ping_record
from .network import load_network
from odmkraken.resources import RESOURCES


@dagster.repository
def busspeeds():
    return [
        mapmatch_bus_data.to_job(resource_defs=RESOURCES, config=mapmatch_config),
        *dagster.with_resources(
            [normalized_ping_record],
            resource_defs={}
        )
        load_network.to_job(resource_defs=RESOURCES)
    ]