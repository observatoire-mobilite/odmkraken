import typing
import dagster
from .mapmatcher import most_likely_path
from odmkraken.busspeeds import extract
from odmkraken.busspeeds.common import busdata_partition
from .network import load_network
from odmkraken.resources import RESOURCES
from odmkraken.resources.pandas import pandas_parquet_manager, icts_data_manager


@dagster.repository
def busspeeds():
    return [
        *dagster.with_resources(
            [
                *dagster.load_assets_from_modules([extract]), 
                most_likely_path
            ],
            resource_defs={'icts_data_manager': icts_data_manager,
                           'pandas_data_manager': pandas_parquet_manager},
            resource_config_by_key={
                'icts_data_manager': { 'config': {
                    'base_path': {'env': 'ICTS_DATA_PATH'}
                }}
            }
        ),
        dagster.define_asset_job(
            name='process_busdata', 
            partitions_def=busdata_partition,
            selection=['duplicate_pings', 'pings', 'pings_from_stops', 'runs', 'most_likely_path'], 
            config={'ops': {'most_likely_path': {'config': {'network_db_dsn': {'env': 'NETWORK_DB_DSN'}}}}}
        ),
        load_network.to_job(resource_defs=RESOURCES)
    ]