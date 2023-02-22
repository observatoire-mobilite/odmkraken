import typing
import dagster
#from .mapmatch import mapmatch_bus_data, mapmatch_config
from .mapmatcher import most_likely_path
from odmkraken.busspeeds import extract  #, mapmatch
from .network import load_network
from odmkraken.resources import RESOURCES
from odmkraken.resources.pandas import pandas_parquet_manager, icts_data_manager


local_icts_data_manager = icts_data_manager.configured({
    'base_path': '/Users/ggeorges/Documents/staat/bus_data'
})


@dagster.repository
def busspeeds():
    return [
        *dagster.with_resources(
            [
                *dagster.load_assets_from_modules([extract]), 
                most_likely_path
            ],
            resource_defs={'icts_data_manager': local_icts_data_manager,
                           'pandas_data_manager': pandas_parquet_manager}
        ),
        load_network.to_job(resource_defs=RESOURCES)
    ]