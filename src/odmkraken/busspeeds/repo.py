import typing
import dagster
from .mapmatch import mapmatch_bus_data
from .extract import icts_data
from .network import load_network
from odmkraken.resources import RESOURCES
from pathlib import Path
import os


def file_sensor_maker(import_job, watch_folder: Path, pattern='*.csv.zip') -> dagster.SensorDefinition:
    
    @dagster.sensor(job=import_job)
    def import_file(context: dagster.SensorEvaluationContext):
        last_mtime = float(context.cursor) if context.cursor else 0

        files = watch_folder.rglob(pattern)
        fstats = [(f, f.stat().st_mtime) for f in files]
        fstats = sorted(fstats, key=lambda f: f[1])
        fstats = [f for f in fstats if f[1] > last_mtime]
        max_mtime = fstats[-1][1]

        for file, mtime in fstats:
            run_key = f'{file}:{str(mtime)}'
            run_config = {'ops': {'extract_from_csv': {'config': str(file)}},
                          'resources': {'local_postgres': {'config': {'dsn': {'env': DSN_EDMO_AOO}}}}}
            yield dagster.RunRequest(run_key=run_key, run_config=run_config)
        context.update_cursor(str(max_mtime))

    return import_file


@dagster.repository
def busspeeds():
    watch_folder = Path(os.getenv('ODMKRAKEN_WATCH_FOLDER', '.'))
        
    assets = [dagster.AssetsDefinition.from_graph(icts_data)]
    jobs = [
        mapmatch_bus_data.to_job(resource_defs=RESOURCES),
        load_network.to_job(resource_defs=RESOURCES)
    ]
    sensors = [file_sensor_maker(jobs[1], watch_folder=watch_folder)]
    return jobs + sensors