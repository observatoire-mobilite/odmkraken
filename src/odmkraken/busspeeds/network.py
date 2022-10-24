"""Load network description."""
import typing
import dagster


@dagster.op(
    required_resource_keys={'local_postgres'},
    config_schema={
        'network_node_file': dagster.StringSource,
    },
)
def load_nodes(context: dagster.OpExecutionContext):
    """Load nodes file."""
    context.log.info('reading nodes file ...')
    table = ('network', 'road_nodes')
    with open(context.op_config['network_node_file'], 'r', encoding='utf8') as buffer:
        context.resources.local_postgres.copy_from(buffer, table)


@dagster.op(
    required_resource_keys={'local_postgres'},
    config_schema={
        'network_edge_file': dagster.StringSource,
    },
)
def load_edges(context: dagster.OpExecutionContext):
    """Load edges file."""
    context.log.info('reading edges file ...')
    table = ('network', 'road_edges')
    with open(context.op_config['network_edge_file'], 'r', encoding='utf8') as buffer:
        context.resources.local_postgres.copy_from(buffer, table)


@dagster.graph()
def load_network():
    """Load complete network model."""
    load_nodes()
    load_edges()
