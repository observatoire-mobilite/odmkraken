-----------------------------------------------------------
-- Trasnform mechanism
-----------------------------------------------------------

/* process:
    1. run `{{vehdata_schema}}.create_staging_table`, creating a temporary table for the raw data - should take a fraction of a second
    2. import the data-file using: `COPY raw_data FROM [file] WITH (FORMAT csv, DELIMITER ';', HEADER 1)` - typically takes a couple of seconds per 100 MB
    3. run the following in no particular order - each step should take around 10s:
        a. `{{vehdata_schema}}.extract_vehicles`
        b. `{{vehdata_schema}}.extract_stops`
        c. `{{vehdata_schema}}.extract_lines`
    4. Only when the three other extract functions are done, run `{{vehdata_schema}}.extract_runs`.
    5. run `b. `{{vehdata_schema}}.extract_pings` - for 300-500 MB files, usually takes around 3-4 minutes
*/
create or replace function {{vehdata_schema}}.create_staging_table()
returns text
as $$
begin
    --drop sequence if exists {{vehdata_schema}}."prog_meter";
    --create sequence {{vehdata_schema}}."prog_meter";

    drop table if exists {{vehdata_schema}}.raw_data;
    create table {{vehdata_schema}}.raw_data (
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
    );
    return '{{vehdata_schema}}."raw_data"';
end
$$ language plpgsql;
comment on function {{vehdata_schema}}.create_staging_table is 'Creates a temporary table to hold the raw data dumps from CSV files.';


create or replace function {{vehdata_schema}}.adjust_format(date_format varchar, input_srid integer default 4326)
returns void
as $$
begin
    alter table {{vehdata_schema}}.raw_data add column zeit timestamp;
    alter table {{vehdata_schema}}.raw_data add column sollzeit timestamp;
    alter table {{vehdata_schema}}.raw_data add column position geometry(POINT, {{system_srid}});
    update {{vehdata_schema}}.raw_data set zeit = to_date("DATUM", date_format) + "ZEIT"::interval where "ZEIT" is not null; -- and nextval('{{vehdata_schema}}.prog_meter') != 0;
    update {{vehdata_schema}}.raw_data set sollzeit = to_date("DATUM", date_format) + "SOLLZEIT"::interval where "SOLLZEIT" is not null; -- and nextval('{{vehdata_schema}}.prog_meter') != 0;
    update {{vehdata_schema}}.raw_data 
        set position = ST_Transform(
            ST_SetSRID(ST_Point(regexp_replace("LONGITUDE", '(\d+)(?:[\.,](\d+))?', '\1.\2')::double precision,
                                regexp_replace("LATITUDE", '(\d+)(?:[\.,](\d+))?', '\1.\2')::double precision), 
                       input_srid),
            {{system_srid}}) where "LONGITUDE" is not null and "LATITUDE" is not null; -- and nextval('{{vehdata_schema}}.prog_meter') != 0;
    alter table {{vehdata_schema}}.raw_data drop column "ZEIT";
    alter table {{vehdata_schema}}.raw_data drop column "SOLLZEIT";
    alter table {{vehdata_schema}}.raw_data drop column "DATUM";
    alter table {{vehdata_schema}}.raw_data drop column "LATITUDE";
    alter table {{vehdata_schema}}.raw_data drop column "LONGITUDE";
end
$$ language plpgsql;


create or replace function {{vehdata_schema}}.extract_vehicles()
returns table(veh_id integer, veh_code integer, veh_plate varchar)
as $$
begin
    return query
    with new_vehicles as (
    insert into {{vehdata_schema}}.vehicles (code, plate, first_seen, original_code)
        select
            substring("FAHRZEUG" from '(\d+)(?: *- *(?:[A-Z]+ *\d+|\w+))?')::integer,
            regexp_replace(substring("FAHRZEUG" from '\d+(?: *- *([A-Z]+ *\d+|\w+))?'), '([A-Z]{2}) *(\d+)', '\1\2'),
            min(zeit) as first_seen,
            "FAHRZEUG" as original_code
        from {{vehdata_schema}}.raw_data
        where "FAHRZEUG" is not null --and nextval('{{vehdata_schema}}.prog_meter') != 0
        group by "FAHRZEUG"
    on conflict (code, plate) do nothing
    returning id as veh_id, code as veh_code, plate as veh_plate
    ) select * from new_vehicles;
end
$$ language plpgsql;
comment on function {{vehdata_schema}}.extract_vehicles is 'Detect if there are any new vehicles in the data-dump and add them if necessary.';


create or replace function {{vehdata_schema}}.extract_stops()
returns table(stop_id integer, stop_code varchar)
as $$
begin
    return query
    with new_stops as (
    insert into {{vehdata_schema}}.stops (code, first_seen)
        select
            "HALT" as code,
            min(zeit) as first_seen
        from {{vehdata_schema}}.raw_data
        where "HALT" is not null -- and nextval('{{vehdata_schema}}.prog_meter') != 0
        group by "HALT"
    on conflict (code) do nothing
    returning id, code
    ) select * from new_stops;
end
$$ language plpgsql;
comment on function {{vehdata_schema}}.extract_stops is 'Detect if there are any new stops in the data-dump and add them if necessary.';


create or replace function {{vehdata_schema}}.extract_lines()
returns table(line_id integer, line_code varchar)
as $$
begin
    return query
    with new_lines as (
    insert into {{vehdata_schema}}.lines (code, first_seen)
    select
        "LINIE",
        min(zeit)
    from {{vehdata_schema}}.raw_data
    where "LINIE" is not null -- and nextval('{{vehdata_schema}}.prog_meter') != 0
    group by "LINIE"
    on conflict(code) do nothing
    returning id, code
    ) select * from new_lines;
end
$$ language plpgsql;
comment on function {{vehdata_schema}}.extract_lines is 'Detect if there are any new lines in the data-dump and add them if necessary.';


create or replace function {{vehdata_schema}}.extract_runs()
returns table(run_vehicle_id integer, run_time_start timestamp, run_time_end timestamp)
as $$
begin
    drop sequence if exists run_counter;
    create temporary sequence run_counter;
    /* if we define runs as set of pings of identical `FAHRZEUG`, `LINIE`, 
    * `FAHRT` and `UMLAUF` then it turns out that `runs` overlap, i.e. it
    * is possible to find two or more runs, where the start or end time
    * of the first falls between the start and end time of the second.
    * Analysis on just one datafile shows: overlaps occur only if either:
    * (1) the end and start time of two subsequent runs happen to 
    * be the same (which means there have to have been two separate messages
    * for the same moment in time) or 
    * (2) one of the runs has no `FAHRT` and/or `LINIE` assigned.
    */

    /* deal with duplicate messages */
    /*select *
    from raw_data
    group by "FAHRZEUG", "DATUM" + "ZEIT"::interval
    having count(*) > 1;
    */

    /*
    * "UMLAUF" is never null, but it may be `0` indicating: not in service
    */
    return query
    with new_runs as (
    insert into {{vehdata_schema}}.runs as r (vehicle_id, line_id, sortie, sortie_flag, run, time_start, time_end)
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
        from {{vehdata_schema}}.raw_data
        left join {{vehdata_schema}}.vehicles v on v.original_code = "FAHRZEUG" 
            -- from '(\d+) *(?:- *(?:[A-Z]+ *\d+|\w+))?')::integer = v.code
            --and regexp_replace("FAHRZEUG", '\d+ *- *(?:(?:([A-Z]+) *(\d+))|(\w+))', '\1\2\3') = v.plate
            --regexp_replace("FAHRZEUG", '(\d+)(?: *- *(?:(?:([A-Z]+) *(\d+))|(\w+)))', '\1 - \2\3\4') = v.code::varchar || ' - ' || v.plate
        left join {{vehdata_schema}}.lines l on l.code = "LINIE"
        order by "FAHRZEUG", zeit
    ) as foo
    --where nextval('{{vehdata_schema}}.prog_meter') != 0
    group by "vehicle_id", run_id, "line_id", "UMLAUF", "FAHRT"
    order by "vehicle_id", "time_start"
    on conflict (vehicle_id, time_start, time_end) do nothing
    returning vehicle_id, time_start, time_end
    ) select * from new_runs;
end
$$ language plpgsql;
comment on function {{vehdata_schema}}.extract_runs is 'Extract vehicle state information as time periods.';


create or replace function {{vehdata_schema}}.extract_runs_with_timeframes()
returns table (vehicle_id integer, time_start timestamp, time_end timestamp)
as $$
select
    run_vehicle_id as vehicle_id,
    min(run_time_start) as time_start,
    max(run_time_end) as time_end
from {{vehdata_schema}}.extract_runs()
group by run_vehicle_id
$$ language SQL;


create or replace function {{vehdata_schema}}.extract_pings()
returns integer
as $$
begin
    insert into {{vehdata_schema}}."pings" (vehicle_id, time, position)
    select
        v.id as vehicle_id,
        zeit as time,
        position
    from {{vehdata_schema}}.raw_data
    left join {{vehdata_schema}}."vehicles" as v on "FAHRZEUG" = v.original_code
    left join {{vehdata_schema}}.stops s on s.code = "HALT"
    where "TYP" = -1
    on conflict(vehicle_id, time) do nothing;

    insert into {{vehdata_schema}}."pings_from_stops" (vehicle_id, time, position, kind, stop_id, 
                                           expected_time, count_people_boarding, count_people_disembarking)
    select
        v.id as vehicle_id,
        zeit as time,
        position,
        {{vehdata_schema}}.event_code_to_event_type("TYP") as kind,
        s.id as stop_id,
        sollzeit as expected_time,
        "EINSTEIGER" as count_people_boarding,
        "AUSSTEIGER" as count_people_disembarking      
    from {{vehdata_schema}}.raw_data
    left join {{vehdata_schema}}."vehicles" as v 
    --on regexp_replace("FAHRZEUG", '(\d+) *- *(\w+)', '\1 - \2') = v.code::varchar || ' - ' || v.plate
    on v.original_code = "FAHRZEUG"
    left join {{vehdata_schema}}.stops s on s.code = "HALT"
    where "TYP" != -1 --and nextval('{{vehdata_schema}}.prog_meter') != 0
    on conflict(vehicle_id, time) do nothing;

    -- delete duplicate entries:
    -- sometimes, there is a ping of TYP >=0 arriving together with a
    -- ping of TYP=-1. Those cause problems, and at least in a few spot
    -- checks the position data was exactly the same. So we drop them here:
    delete from only {{vehdata_schema}}.pings p
    using (
        select v.id as vehicle_id, r.zeit as time
        from {{vehdata_schema}}.raw_data r
        left join {{vehdata_schema}}."vehicles" v on v.original_code = r."FAHRZEUG"
        group by v.id, r.zeit
        having count(*) > 1
    ) as d
    where (p.vehicle_id = d.vehicle_id and p.time = d.time);

    return 0;
end
$$ language plpgsql;
comment on function {{vehdata_schema}}.extract_pings is 'Extract raw ping data - be sure to run the other 4 extract methods first.';
