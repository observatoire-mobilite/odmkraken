import typing
import dagster
from .. import EDMOData
from . import sql
from datetime import datetime, timedelta


# fields: vehicle_id, from_node, to_node, t_enter, dt_travers
RESULT_LIST = typing.List[typing.Tuple[int, int, int, datetime, timedelta]]


class EdmoMapmatchingResults(EDMOData, dagster.IOManager):

    def handle_output(self, context: dagster.OutputContext, data: RESULT_LIST):
        """Store new results in database.

        Beware: the primary key on `vehicle_id` and `t_enter` will cause this to fail
        if you try to overwrite results.
        """
        self.store.execute_batch(sql.add_results(schema=self.vehdata_schema), data)

    def load_input(self, context: dagster.InputContext) -> RESULT_LIST:
        """Read all results from database."""
        return self.store.fetchall(sql.get_results(schema=self.vehdata_schema))


@dagster.io_manager(required_resource_keys={'local_postgres'})
def edmo_mapmatching_results(init_context: dagster.InitResourceContext) -> EdmoMapmatchingResults:
    """IOmanager for mapmatching result data"""
    return EdmoMapmatchingResults(init_context.resources.local_postgres)
