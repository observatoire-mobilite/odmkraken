import typing
import dagster
from .mapmatch import mapmatch_bus_data, mapmatch_config
from .extract import icts_data
from odmkraken.resources import RESOURCES_TEST
from pathlib import Path


def make_file_sensor(job: dagster.JobDefinition, watch_folder: Path=Path()) -> dagster.SensorDefinition:

    @dagster.sensor(job=job, minimum_interval_seconds=60)
    def monitor_upload_folder(context: dagster.SensorEvaluationContext):
        last_mtime = float(context.cursor) if context.cursor else 0
        files = [f for f in watch_folder.glob('*.csv.zip')
                 if f.lstat().st_mtime > last_mtime]
        if not files:
            return dagster.SkipReason('no new files')
        for file in files:
            path = str(file.absolute())
            yield dagster.RunRequest(
                run_key=f'{path}:{file.lstat().st_mtime}',
                run_config={"ops": {"extract_from_csv": {"config": {"file": path}}}}
            )
        max_mtime = max([f.lstat().st_mtime for f in files] + [last_mtime])
        context.update_cursor(str(max_mtime))


    
    return monitor_upload_folder


@dagster.repository
def busspeeds_test():
    icts_data_job = icts_data.to_job(resource_defs=RESOURCES_TEST)
    mapmatch_bus_data_job = mapmatch_bus_data.to_job(
        resource_defs=RESOURCES_TEST,
        config=mapmatch_config
    )
    sensor = make_file_sensor(icts_data_job, watch_folder=Path('/tmp/nginx'))
    return [icts_data_job, mapmatch_bus_data_job, sensor]
