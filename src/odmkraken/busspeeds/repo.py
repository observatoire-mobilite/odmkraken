import typing
import dagster
from .mapmatch import mapmatch_bus_data, mapmatch_config
from .extract import icts_data
from odmkraken.resources import RESOURCES_TEST


@dagster.repository
def busspeeds_test():
    return [mapmatch_bus_data.to_job(
            resource_defs=RESOURCES_TEST,
            config=mapmatch_config),
        icts_data.to_job(
            resource_defs=RESOURCES_TEST
        )
        ]