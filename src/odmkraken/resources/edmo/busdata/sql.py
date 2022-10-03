import typing
from psycopg2.sql import SQL, Identifier, Literal, Composable
import os


class Query:

    def __init__(self, sql: str, **kwargs):
        self.sql = SQL(sql)
        self._defaults = kwargs

    def defaults(self, schema: str='vehdata') -> typing.Iterator[typing.Tuple[str, typing.Union[str, Identifier, Literal]]]:
        for tbl in ('vehicles', 'lines', 'stops', 'runs', 'pings', 'pings_from_halts', 'halts', 'data_files', 'data_file_timeframes'):
            yield (f'{tbl}_table', Identifier(schema, tbl))
        for obj in ('event_code_to_event_type', 'get_vehicle_halts'):
            yield obj, Identifier(schema, obj)
        yield from self._defaults.items()
        
    def __call__(self, schema: str='vehdata', **kwargs):
        for key, val in self.defaults(schema):
            kwargs.setdefault(key, val)
        for key, value in kwargs.items():
            if isinstance(value, Composable):
                continue
            if key.endswith('_table'):
                kwargs[key] = Identifier(schema, value)
            else:
                kwargs[key] = Literal(value)
        return self.sql.format(**kwargs)


create_staging_table = Query('''
drop table if exists {staging_table};
create table {staging_table} (
    "TYP" integer,
    "DATUM" varchar,
    "SOLLZEIT" varchar,
    "ZEIT" varchar,
    "FAHRZEUG" varchar, 
    "LINIE" varchar,
    "UMLAUF" varchar,
    "FAHRT" varchar,
    "HALT" varchar,
    "LATITUDE" varchar,
    "LONGITUDE" varchar,
    "EINSTEIGER" integer,
    "AUSSTEIGER" integer
);''')

len_staging_table = Query('''
select count(*) from {staging_table};
''')

drop_staging_table = Query('''
drop table if exists {staging_table};
''')


adjust_date = Query('''
alter table {staging_table} add column zeit timestamp;
alter table {staging_table} add column sollzeit timestamp;
alter table {staging_table} add column position geometry(POINT, {srid});
update {staging_table} set zeit = to_date("DATUM", {date_format}) + "ZEIT"::interval where "ZEIT" is not null;
update {staging_table} set sollzeit = to_date("DATUM", {date_format}) + "SOLLZEIT"::interval where "SOLLZEIT" is not null;
update {staging_table} 
    set position = ST_Transform(
        ST_SetSRID(ST_Point(regexp_replace("LONGITUDE", '(\d+)(?:[\.,](\d+))?', '\1.\2')::double precision,
                            regexp_replace("LATITUDE", '(\d+)(?:[\.,](\d+))?', '\1.\2')::double precision), 
                    input_srid),
        {srid}) where "LONGITUDE" is not null and "LATITUDE" is not null;
alter table {staging_table} drop column "ZEIT";
alter table {staging_table} drop column "SOLLZEIT";
alter table {staging_table} drop column "DATUM";
alter table {staging_table} drop column "LATITUDE";
alter table {staging_table} drop column "LONGITUDE";
''', srid=Literal(2169))

extract_vehicles = Query('''
with new_vehicles as (
    insert into {vehicles_table} (code, plate, first_seen, original_code)
        select
            substring("FAHRZEUG" from '(\d+)(?: *- *(?:[A-Z]+ *\d+|\w+))?')::integer,
            regexp_replace(substring("FAHRZEUG" from '\d+(?: *- *([A-Z]+ *\d+|\w+))?'), '([A-Z]{2}) *(\d+)', '\1\2'),
            min(zeit) as first_seen,
            "FAHRZEUG" as original_code
        from {staging_table}
        where "FAHRZEUG" is not null
        group by "FAHRZEUG"
    on conflict (code, plate) do nothing
    returning id as veh_id, code as veh_code, plate as veh_plate
) select * from new_vehicles;
''')

extract_stops = Query('''
with new_stops as (
    insert into {stops_table} (code, first_seen)
        select
            "HALT" as code,
            min(zeit) as first_seen
        from {staging_table}
        where "HALT" is not null
        group by "HALT"
    on conflict (code) do nothing
    returning id, code
) select * from new_stops;)
''')

extract_lines = Query('''
with new_lines as (
    insert into {lines_table} (code, first_seen)
    select
        "LINIE",
        min(zeit)
    from {staging_table}
    where "LINIE" is not null
    group by "LINIE"
    on conflict(code) do nothing
    returning id, code
) select line_id, line_code from new_lines;
''')

extract_runs_with_timeframes = Query('''
create temporary sequence run_counter;
select
    run_vehicle_id as vehicle_id,
    min(run_time_start) as time_start,
    max(run_time_end) as time_end
from (
    with new_runs as (
    insert into {runs_table} as r (vehicle_id, line_id, sortie, sortie_flag, run, time_start, time_end)
    select
        vehicle_id,
        line_id,
        case 
            when 
                "UMLAUF" similar to '[A-Z]+[0-9]+'
                then regexp_replace("UMLAUF", '[A-Z]+([0-9]+)', '\1')::integer
            else "UMLAUF"::integer
        end as sortie,
        (regexp_match("UMLAUF", '([A-Z]*)[0-9]+'))[1] as sortie_flag,
        "FAHRT"::integer as run,
        min(zeit) as "time_start",
        max(zeit) as "time_end"
    from (
        select 
            zeit,
            v.id as vehicle_id,
            l.id as line_id,
            "UMLAUF",
            "FAHRT",
            case when
                lag("LINIE") over (order by "FAHRZEUG", zeit) is distinct from "LINIE" or
                lag("UMLAUF") over (order by "FAHRZEUG", zeit) is distinct from "UMLAUF" or
                lag("FAHRT") over (order by "FAHRZEUG", zeit) is distinct from "FAHRT" 
            then nextval('run_counter')
            else currval('run_counter')
            end as run_id
        from {staging_table}
        left join {vehicles_table} v on v.original_code = "FAHRZEUG" 
        left join {lines_table} l on l.code = "LINIE"
        order by "FAHRZEUG", zeit
    ) as foo
    group by "vehicle_id", run_id, "line_id", "UMLAUF", "FAHRT"
    order by "vehicle_id", "time_start"
    on conflict (vehicle_id, time_start, time_end) do nothing
    returning vehicle_id, time_start, time_end
) as runs
group by run_vehicle_id
''')

extract_pings = Query('''
 insert into {pings_table} (vehicle_id, time, position)
    select
        v.id as vehicle_id,
        zeit as time,
        position
    from {staging_table}
    left join {vehicles_table} as v on "FAHRZEUG" = v.original_code
    left join {stops_table} s on s.code = "HALT"
    where "TYP" = -1
    on conflict(vehicle_id, time) do nothing;

    insert into {pings_from_stops_table} (vehicle_id, time, position, kind, stop_id, 
                                          expected_time, count_people_boarding, count_people_disembarking)
    select
        v.id as vehicle_id,
        zeit as time,
        position,
        {event_code_to_event_type}("TYP") as kind,
        s.id as stop_id,
        sollzeit as expected_time,
        "EINSTEIGER" as count_people_boarding,
        "AUSSTEIGER" as count_people_disembarking      
    from {staging_table}
    left join {vehicles_table} as v 
    --on regexp_replace("FAHRZEUG", '(\d+) *- *(\w+)', '\1 - \2') = v.code::varchar || ' - ' || v.plate
    on v.original_code = "FAHRZEUG"
    left join {stops_table} s on s.code = "HALT"
    where "TYP" != -1
    on conflict(vehicle_id, time) do nothing;

    -- delete duplicate entries:
    -- sometimes, there is a ping of TYP >=0 arriving together with a
    -- ping of TYP=-1. Those cause problems, and at least in a few spot
    -- checks the position data was exactly the same. So we drop them here:
    delete from only {pings_table} p
    using (
        select v.id as vehicle_id, r.zeit as time
        from {staging_table} r
        left join {vehicles_table} v on v.original_code = r."FAHRZEUG"
        group by v.id, r.zeit
        having count(*) > 1
    ) as d
    where (p.vehicle_id = d.vehicle_id and p.time = d.time);)
''')


add_data_file = Query('''
insert into {data_files_table} (id, filename, imported_on, checksum) values (gen_random_uuid(), %s, now(), %s) returning id'
''')

add_file_timeframes = Query('''
insert into {data_file_timeframes_table} (id, file_id, vehicle_id, time_start, time_end) values (gen_random_uuid(), %s, %s, %s, %s);'
''')

vehicle_timeframes_for_file = Query('''
select 
    id, vehicle_id, time_start, time_end
from vehdata.data_file_timeframes
where file_id=%s''')

vehicle_timeframes_for_period = Query('''
select
    id, vehicle_id, time_start, time_end
from {data_file_timeframes_table}
where time_start between %s and %s''')


file_by_checksum = Query('''
select * from {data_files_table} where checksum=%s
''')


extract_halts = Query('''
insert into {halts_table}
select * from {get_vehicle_halts}(%s, %s, %s)
''')

