-- kill anything that survived previous attempts
drop table if exists {{network_schema}}."road_edges";
drop table if exists {{network_schema}}."road_edges_overlay_bus"; 
drop table if exists {{network_schema}}."road_nodes";
drop type if exists {{network_schema}}."road_edge_allowed_directions";


-- start recreating ...
create schema if not exists {{network_schema}};
comment on schema {{network_schema}} is 'road transport network topology';
grant usage, create on schema {{network_schema}} to {{user_owner}};
grant usage on schema {{network_schema}} to {{user_aoo}};
grant usage on schema {{network_schema}} to {{user_ro}};
alter default privileges in schema {{network_schema}} grant UPDATE, INSERT, SELECT, DELETE on tables to {{user_owner}};
alter default privileges in schema {{network_schema}} grant UPDATE, INSERT, SELECT, DELETE on tables to {{user_aoo}};
alter default privileges in schema {{network_schema}} grant SELECT on tables to {{user_ro}};


-- types to describe allowed directions
create type {{network_schema}}."road_edge_allowed_directions"
as ENUM ('both', 'forwards', 'backwards', 'none', 'undetermined');

-- node table
create table {{network_schema}}."road_nodes" (
    id integer primary key,
    geometry Geometry(POINT, {{system_srid}})
);
create index idx_road_nodes_gix on {{network_schema}}."road_nodes" using GIST ("geometry");


-- edge table (links nodes)
create table {{network_schema}}."road_edges" (
    id integer primary key,
    original_tomtom_id bigint,
    from_node integer, --references {{network_schema}}."road_nodes" (id),
    to_node integer, -- references {{network_schema}}."road_nodes" (id),
    allowed_directions {{network_schema}}."road_edge_allowed_directions",
    hierarchy smallint,
    geometry Geometry(LINESTRING, {{system_srid}})
);
create index idx_road_edges_gix on {{network_schema}}."road_edges" using GIST ("geometry");


create table {{network_schema}}."road_edges_overlay_bus" (
    id integer primary key,
    allowed_directions {{network_schema}}."road_edge_allowed_directions"
);
comment on table {{network_schema}}."road_edges_overlay_bus" is 'This table allows to overwrite the `allowed_directions` in `road_edges` when used through `get_edgelist`';

----------
drop function if exists {{network_schema}}."get_edgelist";
create function {{network_schema}}."get_edgelist"() 
returns table(id integer,
              source integer,
              target integer,
              weight double precision,
              forward boolean
             ) 
as $$

with bus_network as (
    select 
        e.id, 
        from_node,
        to_node,
        st_length(geometry) as weight,
        coalesce(o."allowed_directions", e."allowed_directions", 'undetermined'::{{network_schema}}."road_edge_allowed_directions") as allowed_directions
    from {{network_schema}}."road_edges" e
    left join {{network_schema}}."road_edges_overlay_bus" o on o.id = e.id
)

(
    select 
        id, 
        from_node as source, 
        to_node as target,
        weight,
        True as forward
    from bus_network
    where "allowed_directions" in ('forwards', 'both', 'undetermined')
) union (
    select 
        id,
        to_node as source, 
        from_node as target,
        weight,
        False as forward
    from bus_network
    where "allowed_directions" in ('backwards', 'both', 'undetermined')
)

$$ language SQL;
comment on function {{network_schema}}."get_edgelist" is 'get navigation graph for shortest path calculation';

-------------
drop function if exists {{network_schema}}."nearby_roads";
create function {{network_schema}}."nearby_roads"(double precision, double precision, double precision default 15*6) 
returns table(source int, target int, -- nodes at either end of the segment
              length double precision, -- length of the geometry
              "position" double precision, -- closest relative position on geometry
              heading double precision, -- azimuth of tangent in direction of geometry
              error double precision -- distance from point to geometry
             ) 
as $$

with point as (
    -- convert coordinates to point geometry
    select st_setsrid(st_point($1, $2), {{system_srid}}) as geometry
), candidates as ( 
    -- find geometries within $3 meters around `point.geometry`
    /* *approach*: the `road_edge` table stores road geometries. The geometries are
        LINESTRINGs, which are sequences of points. Thus there is an implied "forward" 
        direction for every road. This first query extracts all road geometries 
        within a distance of $3 of our point ($1, $2), without caring about the
        vehicle's direction of travel. For those metrics, namely `heading` that imply
        a direction of travel, we presume the vehicle is travelling in "forward" direction.
    */
    select  
        e.from_node as source,
        e.to_node as target,
        st_length(e.geometry) as length,
        st_linelocatepoint(e.geometry, p.geometry) as position, -- position from starting point of geometry (regardless of direction of travel)
        -- azimuth at the projected point on the geometry, presuming the vehicle travels in "forwards" direction of the geometry
        -- unless we are at the very end of the linestring, the azimuth is measured between project point on the geometry and a point 0.001 relative length units further "forwards"
        -- if we are at relative position 0.999 or further in forward direction, just use the azimuth between position 0.999 and the geometry's end point
        case when st_linelocatepoint(e.geometry, p.geometry) < 0.999
            then st_azimuth(st_closestpoint(e.geometry, p.geometry), 
                            st_lineinterpolatepoint(e.geometry, st_linelocatepoint(e.geometry, p.geometry) + 0.001))
            else st_azimuth(st_lineinterpolatepoint(e.geometry, 0.999), 
                            st_endpoint(e.geometry))
        end as heading,
        st_distance(e.geometry, p.geometry) as error,
        allowed_directions
    from
        {{network_schema}}."road_edges" e,
        point p
    where
        st_dwithin(e.geometry, p.geometry, $3)
        and (allowed_directions != 'none')  -- discard geometries we can't travel on
        -- only chose geometries if the point ($1, $2) lies to the left or right of the linestring, not "before" or "after"
        and st_linelocatepoint(e.geometry, p.geometry) > 0
        and st_linelocatepoint(e.geometry, p.geometry) < 1
)

/* move from "geometries" to "edges", i.e. describe links in terms of their origin and destination nodes, 
   as a function of the directions the geometry may be legally traversed. */
select 
    source, target, 
    length, position, error, 
    heading 
from candidates
where allowed_directions != 'backwards'
union
select
    target as source, source as target,
    length, 1 - position, error,
    case when heading > pi() then heading - pi() else heading + pi() end as heading
from candidates
where allowed_directions != 'forwards'

$$ language SQL;
comment on function {{network_schema}}."nearby_roads" is 'get roads closest to specified coordinates';


-----------------------------------------------------------
-- database setup
-----------------------------------------------------------
/* kill anything that might come in our way */
drop function if exists {{vehicle_schema}}.get_pings;
drop table if exists {{vehicle_schema}}.pings_from_stops;
drop table if exists {{vehicle_schema}}.pings;
drop table if exists {{vehicle_schema}}.runs;
drop table if exists {{vehicle_schema}}.lines;
drop table if exists {{vehicle_schema}}.vehicles;
drop table if exists {{vehicle_schema}}.stops;
drop function if exists {{vehicle_schema}}.extract_pings;
drop function if exists {{vehicle_schema}}.extract_runs_with_timeframes;
drop function if exists {{vehicle_schema}}.extract_runs;
drop function if exists {{vehicle_schema}}.extract_lines;
drop function if exists {{vehicle_schema}}.extract_vehicles;
drop function if exists {{vehicle_schema}}.extract_stops;
drop function if exists {{vehicle_schema}}.create_staging_table;
drop function if exists {{vehicle_schema}}.event_code_to_event_type;
drop function if exists {{vehicle_schema}}.event_type_to_event_code;
drop type if exists {{vehicle_schema}}."event_type";
drop schema if exists {{vehicle_schema}};

/* schema */
create schema {{vehicle_schema}};
comment on schema {{vehicle_schema}} is 'raw data from the init tracking system for vehicles of public transport';
grant usage, create on schema {{vehicle_schema}} to {{user_owner}};
grant usage, create on schema {{vehicle_schema}} to {{user_aoo}};  -- needs create for sequences and temp tables; should it though ??
grant usage on schema {{vehicle_schema}} to {{user_ro}};
alter default privileges in schema {{vehicle_schema}} grant ALL on tables to {{user_owner}};
alter default privileges in schema {{vehicle_schema}} grant UPDATE, INSERT, SELECT, DELETE on tables to {{user_aoo}};
alter default privileges in schema {{vehicle_schema}} grant USAGE, SELECT, UPDATE on sequences to {{user_aoo}};
alter default privileges in schema {{vehicle_schema}} grant SELECT on tables to {{user_ro}};

{% set EVENT_TYPE_CODES = {
    -1: 'ping', 
    0: 'geplante Haltestelle + Fahrplanpunkt',
    1: 'Bedarfshaltestelle (geplant)',
    2: 'ungeplante Haltestelle + Tür offen',
    3: 'Störungspunkt',
    4: 'Durchfahrt ohne Fahrgastaufnahme',
    5: 'Haltestelle + kein Fahrplanpunkt',
    6: 'Durchfahrt ohne Fahrgastaufnahme oder Fahrplanpunkt'}
%}

/* enum encoding possible types of events */
create type {{vehicle_schema}}."event_type" as enum (
    {%- for value in EVENT_TYPE_CODES.values() %}
    {{value | literal}}{% if not loop.last %},{% endif %}
    {%- endfor %}
);
comment on type {{vehicle_schema}}."event_type" is 'All possible types of messages received from vehicles.';

/* function to encode init codes to the string version of the enum */
create or replace function {{vehicle_schema}}."event_code_to_event_type"(code integer)
returns {{vehicle_schema}}."event_type" as $$
declare
    answer {{vehicle_schema}}."event_type";
begin
    case code
        {%- for code, value in EVENT_TYPE_CODES.items() %}
        when {{code | literal}} then answer := {{value | literal}};
        {%- endfor -%}
        else
            raise exception 'invalid input --> %', integer
                USING HINT = 'acceptable values are: {% for code in EVENT_TYPE_CODES.keys()%}{{code}}{% if not loop.last %},{% endif %}{% endfor %}';
    end case;
    return answer; 
end;
$$ language plpgsql;
comment on function {{vehicle_schema}}."event_code_to_event_type" is 'Convert integer representation of categorical `kind` variable back to textual form, encoded by the enum `event_type`.';

/* reverse: from string version of the enum to codes */
create or replace function {{vehicle_schema}}."event_type_to_event_code"(event {{vehicle_schema}}."event_type")
returns integer as $$
declare
    answer integer;
begin
    case event
        {%- for code, value in EVENT_TYPE_CODES.items() %}
        when {{value | literal}} then answer := {{code | literal}};
        {%- endfor %}
        else
            raise exception 'invalid input --> %', integer
                USING HINT = 'acceptable values are: {% for code in EVENT_TYPE_CODES.values()%}{{code}}{% if not loop.last %},{% endif %}{% endfor %}';
    end case;
    return answer; 
end;
$$ language plpgsql;
comment on function {{vehicle_schema}}."event_type_to_event_code" is 'Convert textual representation of categorical `kind` variable back to integer.';

/* lines table: known line services of public transport */
create table if not exists {{vehicle_schema}}.lines (
    id serial primary key,
    code varchar unique not null,
    first_seen timestamp
);
comment on table {{vehicle_schema}}.lines is 'Known line services of public transport.';
comment on column {{vehicle_schema}}.lines.id is 'Internal identifier, uniquely identifying every known line service; assigned while importing on first occurence in the data (primary key).';
comment on column {{vehicle_schema}}.lines.code is 'Identifier assigned by the tracking system, uniquely identifying a given line service on a given day.';
comment on column {{vehicle_schema}}.lines.first_seen is 'Time and date of first detection of this line in the data.';

/* vehicles table: known vehicles on the network */
create table if not exists {{vehicle_schema}}.vehicles (
    id serial primary key,
    code integer,
    plate varchar check (plate ~ '([A-Z]*\d+|[a-zA-Z0-9 ]+)'),
    first_seen timestamp,
    original_code varchar,
    unique(code, plate)
);
comment on table {{vehicle_schema}}.vehicles is 'Known vehicles on the network.';
comment on column {{vehicle_schema}}.vehicles.id is 'Internal identifier, uniquely identifying every known vehicle; assigned while importing, on first occurence in the data (primary key).';
comment on column {{vehicle_schema}}.vehicles.code is 'Numeric portion of the vehicle identifier used by the tracking system. Note: the combination of `code` and `plate` has to be unique.';
comment on column {{vehicle_schema}}.vehicles.plate is 'Licence plate number of the vehicle or alpha-numeric part of the code used by the tracking system. Note: the combination of `code` and `plate` has to be unique.';
comment on column {{vehicle_schema}}.vehicles.first_seen is 'Time and date of first detection of this vehicle in the data.';

/* stops table: known stops on the network */
create table if not exists {{vehicle_schema}}.stops (
    id serial primary key,
    code varchar unique,
    first_seen timestamp
);
comment on table {{vehicle_schema}}.stops is 'Known stops on the network.';
comment on column {{vehicle_schema}}.stops.id is 'Internal identifier, uniquely identifying every known stop; assigned while importing, on first occurence in the data (primary key).';
comment on column {{vehicle_schema}}.stops.code is 'Identifier assigned by the tracking system, uniquely identifying a given stop on a given day.';
comment on column {{vehicle_schema}}.stops.first_seen is 'Time and date of first detection of this stop in the data.';

/* runs table: service state of a vehicle at a given time */
create table if not exists {{vehicle_schema}}.runs (
    vehicle_id integer references {{vehicle_schema}}.vehicles(id),
    line_id integer references {{vehicle_schema}}.lines(id),
    sortie integer,
    sortie_flag text,
    run integer,
    time_start timestamp,
    time_end timestamp,
    primary key (vehicle_id, time_start, time_end)
);
comment on table {{vehicle_schema}}.runs is 'Runs performed by vehicles: period where the vehicle displays the same combination of line_id, sortie and run over a continguous sequence of messages. Primary key: vehicle_id, time_start and time_end';
comment on column {{vehicle_schema}}.runs.vehicle_id is 'Identity of the vehicle';
comment on column {{vehicle_schema}}.runs.line_id is 'Line service the vehicle reported performing. May be null, indicating: no line displayed.';
comment on column {{vehicle_schema}}.runs.sortie is 'Numerical identifier of the pre-programmed course. If 0, the vehicle is not operating as a vehicle of public transport.';
comment on column {{vehicle_schema}}.runs.sortie_flag is 'Weird textual prefix that comes with some `UMLAUF`.';
comment on column {{vehicle_schema}}.runs.run is 'Numerical identifier of the lag of a sortie a vehicle is on. May be null, indicating the vehicle is between legs or, if sortie is 0, not in service.';
comment on column {{vehicle_schema}}.runs.time_start is 'Beginning of run (first in a continguous sequence of messages sharing the same combination of values for line_id, sortie and run)';
comment on column {{vehicle_schema}}.runs.time_end is 'End of run (last in a continguous sequence of messages sharing the same combination of values for line_id, sortie and run)';

/* pings table: log of received messages */
create table if not exists {{vehicle_schema}}."pings" (
    vehicle_id integer references {{vehicle_schema}}.vehicles(id),
    time timestamp,
    position geometry(POINT, {{system_srid}}),
    primary key (vehicle_id, time)
);
comment on table {{vehicle_schema}}.pings is 'Position reports received from the vehicles. Note: for this table, all pings are presumed of type `ping`.';
comment on column {{vehicle_schema}}.pings.vehicle_id is 'Identity of the vehicle';
comment on column {{vehicle_schema}}.pings.time is 'Moment in time the message was emitted.';
comment on column {{vehicle_schema}}.pings.time is 'The point position reported by the vehicle.';

/* pings_from_stops: pings carrying information on a stop */
create table if not exists {{vehicle_schema}}."pings_from_stops" (
    kind {{vehicle_schema}}."event_type" check (kind != 'ping'),
    stop_id integer references {{vehicle_schema}}.stops(id),
    expected_time timestamp,
    count_people_boarding integer,
    count_people_disembarking integer,
    primary key (vehicle_id, time)
) inherits ({{vehicle_schema}}."pings");
comment on table {{vehicle_schema}}.pings_from_stops is 'Position reports including information only available if the vehicle stops and opens its doors. This may or happen at the times and places designated by the official schedule, or at any other place and time.';
comment on column {{vehicle_schema}}.pings_from_stops.kind is 'The kind of message sent (see type enum {{vehicle_schema}}.event_type)';
comment on column {{vehicle_schema}}.pings_from_stops.stop_id is 'Identity of the stop. Only set if the vehicle stops at the designated time and location according to the official schedule.';
comment on column {{vehicle_schema}}.pings_from_stops.expected_time is 'Time the vehicle should have reported its position according to the schedule. Note: the reference is the time of departure. Also note: this is not set if the vehicle makes an impromptu stop.';
comment on column {{vehicle_schema}}.pings_from_stops.count_people_boarding is 'Number of people boarding the vehicle, as detected by the onboard counting equipment. Note: only set if the vehicle is equiped and the equipment is operational.';
comment on column {{vehicle_schema}}.pings_from_stops.count_people_disembarking is 'Number of people disembarking the vehicle, as detected by the onboard counting equipment . Note: only set if the vehicle is equiped and the equipment is operational.';


-----------------------------------------------------------
-- Trasnform mechanism
-----------------------------------------------------------

/* process:
    1. run `{{vehicle_schema}}.create_staging_table`, creating a temporary table for the raw data - should take a fraction of a second
    2. import the data-file using: `COPY raw_data FROM [file] WITH (FORMAT csv, DELIMITER ';', HEADER 1)` - typically takes a couple of seconds per 100 MB
    3. run the following in no particular order - each step should take around 10s:
        a. `{{vehicle_schema}}.extract_vehicles`
        b. `{{vehicle_schema}}.extract_stops`
        c. `{{vehicle_schema}}.extract_lines`
    4. Only when the three other extract functions are done, run `{{vehicle_schema}}.extract_runs`.
    5. run `b. `{{vehicle_schema}}.extract_pings` - for 300-500 MB files, usually takes around 3-4 minutes
*/
create or replace function {{vehicle_schema}}.create_staging_table()
returns text
as $$
begin
    --drop sequence if exists {{vehicle_schema}}."prog_meter";
    --create sequence {{vehicle_schema}}."prog_meter";

    drop table if exists {{vehicle_schema}}.raw_data;
    create table {{vehicle_schema}}.raw_data (
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
    return '{{vehicle_schema}}."raw_data"';
end
$$ language plpgsql;
comment on function {{vehicle_schema}}.create_staging_table is 'Creates a temporary table to hold the raw data dumps from CSV files.';


create or replace function {{vehicle_schema}}.adjust_format(date_format varchar, input_srid integer default 4326)
returns void
as $$
begin
    alter table {{vehicle_schema}}.raw_data add column zeit timestamp;
    alter table {{vehicle_schema}}.raw_data add column sollzeit timestamp;
    alter table {{vehicle_schema}}.raw_data add column position geometry(POINT, {{system_srid}});
    update {{vehicle_schema}}.raw_data set zeit = to_date("DATUM", date_format) + "ZEIT"::interval where "ZEIT" is not null; -- and nextval('{{vehicle_schema}}.prog_meter') != 0;
    update {{vehicle_schema}}.raw_data set sollzeit = to_date("DATUM", date_format) + "SOLLZEIT"::interval where "SOLLZEIT" is not null; -- and nextval('{{vehicle_schema}}.prog_meter') != 0;
    update {{vehicle_schema}}.raw_data 
        set position = ST_Transform(
            ST_SetSRID(ST_Point(regexp_replace("LONGITUDE", '(\d+)(?:[\.,](\d+))?', '\1.\2')::double precision,
                                regexp_replace("LATITUDE", '(\d+)(?:[\.,](\d+))?', '\1.\2')::double precision), 
                       input_srid),
            {{system_srid}}) where "LONGITUDE" is not null and "LATITUDE" is not null; -- and nextval('{{vehicle_schema}}.prog_meter') != 0;
    alter table {{vehicle_schema}}.raw_data drop column "ZEIT";
    alter table {{vehicle_schema}}.raw_data drop column "SOLLZEIT";
    alter table {{vehicle_schema}}.raw_data drop column "DATUM";
    alter table {{vehicle_schema}}.raw_data drop column "LATITUDE";
    alter table {{vehicle_schema}}.raw_data drop column "LONGITUDE";
end
$$ language plpgsql;


create or replace function {{vehicle_schema}}.extract_vehicles()
returns table(veh_id integer, veh_code integer, veh_plate varchar)
as $$
begin
    return query
    with new_vehicles as (
    insert into {{vehicle_schema}}.vehicles (code, plate, first_seen, original_code)
        select
            substring("FAHRZEUG" from '(\d+)(?: *- *(?:[A-Z]+ *\d+|\w+))?')::integer,
            regexp_replace(substring("FAHRZEUG" from '\d+(?: *- *([A-Z]+ *\d+|\w+))?'), '([A-Z]{2}) *(\d+)', '\1\2'),
            min(zeit) as first_seen,
            "FAHRZEUG" as original_code
        from {{vehicle_schema}}.raw_data
        where "FAHRZEUG" is not null --and nextval('{{vehicle_schema}}.prog_meter') != 0
        group by "FAHRZEUG"
    on conflict (code, plate) do nothing
    returning id as veh_id, code as veh_code, plate as veh_plate
    ) select * from new_vehicles;
end
$$ language plpgsql;
comment on function {{vehicle_schema}}.extract_vehicles is 'Detect if there are any new vehicles in the data-dump and add them if necessary.';


create or replace function {{vehicle_schema}}.extract_stops()
returns table(stop_id integer, stop_code varchar)
as $$
begin
    return query
    with new_stops as (
    insert into {{vehicle_schema}}.stops (code, first_seen)
        select
            "HALT" as code,
            min(zeit) as first_seen
        from {{vehicle_schema}}.raw_data
        where "HALT" is not null -- and nextval('{{vehicle_schema}}.prog_meter') != 0
        group by "HALT"
    on conflict (code) do nothing
    returning id, code
    ) select * from new_stops;
end
$$ language plpgsql;
comment on function {{vehicle_schema}}.extract_stops is 'Detect if there are any new stops in the data-dump and add them if necessary.';


create or replace function {{vehicle_schema}}.extract_lines()
returns table(line_id integer, line_code varchar)
as $$
begin
    return query
    with new_lines as (
    insert into {{vehicle_schema}}.lines (code, first_seen)
    select
        "LINIE",
        min(zeit)
    from {{vehicle_schema}}.raw_data
    where "LINIE" is not null -- and nextval('{{vehicle_schema}}.prog_meter') != 0
    group by "LINIE"
    on conflict(code) do nothing
    returning id, code
    ) select * from new_lines;
end
$$ language plpgsql;
comment on function {{vehicle_schema}}.extract_lines is 'Detect if there are any new lines in the data-dump and add them if necessary.';


create or replace function {{vehicle_schema}}.extract_runs()
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
    insert into {{vehicle_schema}}.runs as r (vehicle_id, line_id, sortie, sortie_flag, run, time_start, time_end)
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
        from {{vehicle_schema}}.raw_data
        left join {{vehicle_schema}}.vehicles v on v.original_code = "FAHRZEUG" 
            -- from '(\d+) *(?:- *(?:[A-Z]+ *\d+|\w+))?')::integer = v.code
            --and regexp_replace("FAHRZEUG", '\d+ *- *(?:(?:([A-Z]+) *(\d+))|(\w+))', '\1\2\3') = v.plate
            --regexp_replace("FAHRZEUG", '(\d+)(?: *- *(?:(?:([A-Z]+) *(\d+))|(\w+)))', '\1 - \2\3\4') = v.code::varchar || ' - ' || v.plate
        left join {{vehicle_schema}}.lines l on l.code = "LINIE"
        order by "FAHRZEUG", zeit
    ) as foo
    --where nextval('{{vehicle_schema}}.prog_meter') != 0
    group by "vehicle_id", run_id, "line_id", "UMLAUF", "FAHRT"
    order by "vehicle_id", "time_start"
    on conflict (vehicle_id, time_start, time_end) do nothing
    returning vehicle_id, time_start, time_end
    ) select * from new_runs;
end
$$ language plpgsql;
comment on function {{vehicle_schema}}.extract_runs is 'Extract vehicle state information as time periods.';


create or replace function {{vehicle_schema}}.extract_runs_with_timeframes()
returns table (vehicle_id integer, time_start timestamp, time_end timestamp)
as $$
select
    run_vehicle_id as vehicle_id,
    min(run_time_start) as time_start,
    max(run_time_end) as time_end
from {{vehicle_schema}}.extract_runs()
group by run_vehicle_id
$$ language SQL;


create or replace function {{vehicle_schema}}.extract_pings()
returns integer
as $$
begin
    insert into {{vehicle_schema}}."pings" (vehicle_id, time, position)
    select
        v.id as vehicle_id,
        zeit as time,
        position
    from {{vehicle_schema}}.raw_data
    left join {{vehicle_schema}}."vehicles" as v on "FAHRZEUG" = v.original_code
    left join {{vehicle_schema}}.stops s on s.code = "HALT"
    where "TYP" = -1
    on conflict(vehicle_id, time) do nothing;

    insert into {{vehicle_schema}}."pings_from_stops" (vehicle_id, time, position, kind, stop_id, 
                                           expected_time, count_people_boarding, count_people_disembarking)
    select
        v.id as vehicle_id,
        zeit as time,
        position,
        {{vehicle_schema}}.event_code_to_event_type("TYP") as kind,
        s.id as stop_id,
        sollzeit as expected_time,
        "EINSTEIGER" as count_people_boarding,
        "AUSSTEIGER" as count_people_disembarking      
    from {{vehicle_schema}}.raw_data
    left join {{vehicle_schema}}."vehicles" as v 
    --on regexp_replace("FAHRZEUG", '(\d+) *- *(\w+)', '\1 - \2') = v.code::varchar || ' - ' || v.plate
    on v.original_code = "FAHRZEUG"
    left join {{vehicle_schema}}.stops s on s.code = "HALT"
    where "TYP" != -1 --and nextval('{{vehicle_schema}}.prog_meter') != 0
    on conflict(vehicle_id, time) do nothing;

    -- delete duplicate entries:
    -- sometimes, there is a ping of TYP >=0 arriving together with a
    -- ping of TYP=-1. Those cause problems, and at least in a few spot
    -- checks the position data was exactly the same. So we drop them here:
    delete from only {{vehicle_schema}}.pings p
    using (
        select v.id as vehicle_id, r.zeit as time
        from {{vehicle_schema}}.raw_data r
        left join {{vehicle_schema}}."vehicles" v on v.original_code = r."FAHRZEUG"
        group by v.id, r.zeit
        having count(*) > 1
    ) as d
    where (p.vehicle_id = d.vehicle_id and p.time = d.time);

    return 0;
end
$$ language plpgsql;
comment on function {{vehicle_schema}}.extract_pings is 'Extract raw ping data - be sure to run the other 4 extract methods first.';

/* mapmatching functions */
create function {{vehicle_schema}}.get_pings(veh_id int, time_begin timestamp, time_end timestamp)
returns table(x double precision,
              y double precision,
              "time" timestamp) 
as $$
select 
    st_x(position),
    st_y(position),
    time
from {{vehicle_schema}}.pings
where 
    vehicle_id = veh_id
    and time between time_begin and time_end
    and position is not null
order by time
$$ language SQL;

drop table if exists {{vehicle_schema}}."results";
create table {{vehicle_schema}}."results" (
    vehicle_id integer,
    from_node integer,
    to_node integer,
    t_enter timestamp,
    dt_travers interval
);


/* halting at bus stops */
create or replace function {{vehicle_schema}}.get_vehicle_halts(veh_id int, time_begin timestamp, time_end timestamp)
returns table(vehicle_id int,
              "time" timestamp,
              "position" geometry,
              stop_id int,
              scheduled_time timestamp,
	          dt_stationary interval,
              count_people_boarding int,
              count_people_disembarking int) 
as $$
select 
	vehicle_id, time, position, stop_id, expected_time as scheduled_time,
	dt_stationary, count_people_boarding, count_people_disembarking
from (
	select 
		vehicle_id, time, position, kind, stop_id, expected_time, count_people_boarding, count_people_disembarking,
		time - lag(time) over (partition by vehicle_id order by vehicle_id, time) as dt_stationary
	from bus_data.pings as p
	natural inner join bus_data.pings_from_stops as s
    where vehicle_id = veh_id and time between time_begin and time_end
) as foo where kind is not null and kind != 'Störungspunkt'
$$ language SQL;
comment on function {{vehicle_schema}}.get_vehicle_halts is 'Retrieves information about the halts that vehicle `vehicle_id` makes at known (or unknown) stops of the public transport network within the period between `time_begin` and `time_end`';

create table if not exists {{vehicle_schema}}.halts (
    vehicle_id int references {{vehicle_schema}}.vehicles(id),
    "time" timestamp,
    "position" geometry(POINT, {{system_srid}}),
    stop_id int references {{vehicle_schema}}.stops(id),
    scheduled_time timestamp,
    dt_stationary interval,
    count_people_boarding int,
    count_people_disembarking int,
    primary key (vehicle_id, "time")
);
comment on table {{vehicle_schema}}.halts is 'A halt is defined as a vehicle stopping and opening its doors to let passengers in or out. Each row in this tables corresponds to the vehicle closing its doors and driving away again after halting (obviously the passenger count is only defined after everybody got in or out).';
comment on column {{vehicle_schema}}.halts.vehicle_id is 'Vehicle that reported the halt.';
comment on column {{vehicle_schema}}.halts.time is 'Time at which the vehicle reported its halt. This is always the end of the halt, i.e. as the vehicle closes its doors and drives away.';
comment on column {{vehicle_schema}}.halts.position is 'Position as reported by the vehicle''s GNSS receiver. Theoretically, the field should always be set. Practically, < 1% of pings (of any kind) have no spatial information. Mapmatching ignores them, but those sent during halts will pop up here (meaning this field may be null). Note that the reported position may deviate from the official coordinates of the stop (if reported).';
comment on column {{vehicle_schema}}.halts.stop_id is 'The stop reported by the vehicle. If not set, the halt occured outside the official stop network.';
comment on column {{vehicle_schema}}.halts.scheduled_time is 'Indicates the time at which a vehicle was supposed to halt according to its schedule. If not set, the halt was unscheduled.';
comment on column {{vehicle_schema}}.halts.dt_stationary is 'Time the vehicle spent standing still. Note that since `time` refers to the end of a halt, `dt_stationary` measures the time backwards to the moment the vehicle physically stopped moving.';
comment on column {{vehicle_schema}}.halts.count_people_boarding is 'Number of people that boarded the bus while it stood still. If not set, the vehicle had no functional counting equipment during its halt. Beware: counters may miss passengers, especially at the terminus (while the engine is shut off).';
comment on column {{vehicle_schema}}.halts.count_people_disembarking is 'Number of people that left the bus while it stood still. If not set, the vehicle had no functional counting equipment during its halt. Beware: counters may miss passengers, especially at the terminus (while the engine is shut off).';


drop table if exists {{vehicle_schema}}."results";
create table {{vehicle_schema}}."results" (
    vehicle_id integer references {{vehicle_schema}}."vehicles"(id),
    from_node integer references {{network_schema}}."road_nodes"(id),
    to_node integer references {{network_schema}}."road_nodes"(id),
    t_enter timestamp,
    dt_travers interval,
    primary key (vehicle_id, t_enter)
);
comment on table {{vehicle_schema}}."results" is 'Trajectories reprojected onto a road-edge basis. Each row describes a vehicle passing over the specified link at and within a given time.';
comment on column {{vehicle_schema}}."results"."vehicle_id" is 'The identity of the vehicle as objserved.';
comment on column {{vehicle_schema}}."results"."from_node" is 'Node from which the vehicle entered the edge.';
comment on column {{vehicle_schema}}."results"."to_node" is 'Node at which the vehicle exited the edge (or at least the direction into which it drove).';
comment on column {{vehicle_schema}}."results"."t_enter" is 'Absolute time of the entry at `from_edge`.';
comment on column {{vehicle_schema}}."results"."dt_travers" is 'Time spent on the link, including with the vehicle at rest.';

CREATE OR REPLACE VIEW {{vehicle_schema}}.results_with_runs
 AS
 SELECT d.vehicle_id,
    d.t_enter,
    d.dt_travers,
    d.from_node,
    d.to_node,
    r.line_id,
    r.sortie,
    r.sortie_flag,
    r.run
   FROM {{vehicle_schema}}.results d
     LEFT JOIN bus_data.runs r ON d.t_enter >= r.time_start AND d.t_enter <= r.time_end AND d.vehicle_id = r.vehicle_id
  ORDER BY d.vehicle_id, d.t_enter;

comment on view {{vehicle_schema}}.results_with_runs is 'Flattened representation of `{{vehicle_schema}}.results` table, i.e. including all meta data akin to the input CSV files. Each line represents one passage of a vehicle over a road link.';


CREATE OR REPLACE VIEW {{vehicle_schema}}.results_with_runs_and_geometries
 AS
 SELECT r.vehicle_id,
    r.t_enter,
    r.dt_travers,
    e.geometry AS road_geometry,
    r.from_node = e.from_node AS road_forwards,
    l.code AS line,
    r.run,
    r.sortie_flag,
    r.sortie
   FROM {{vehicle_schema}}.results_with_runs r
     LEFT JOIN {{network_schema}}.road_edges e ON e.from_node = r.from_node AND e.to_node = r.to_node OR e.to_node = r.from_node AND e.from_node = r.to_node
     LEFT JOIN {{vehicle_schema}}.lines l ON r.line_id = l.id;

comment on view {{vehicle_schema}}.results_with_runs_and_geometries is 'Same as `{{vehicle_schema}}.results_with_runs` (flattened representation of `{{vehicle_schema}}.results` table, including all meta-data) but including the actual geometry instead of just its `from_node` and `to_node`. Note: the `road_forwards` field indicates the direction of travel (relative to the geometry) instead of the `from_node` and `to_node` fields.';


-- View: bus_data.map_static_stats_by_edge
DROP MATERIALIZED VIEW if exists {{vehicle_schema}}.map_static_stats_by_edge;
CREATE MATERIALIZED VIEW {{vehicle_schema}}.map_static_stats_by_edge
AS
  WITH results AS (
    SELECT results.from_node,
      results.to_node,
      avg(results.dt_travers) AS dt_stay_avg,
      count(*) AS n_sightings
      FROM {{vehicle_schema}}.results
    GROUP BY results.from_node, results.to_node
  )
  SELECT r.from_node,
    r.to_node,
    e.geometry,
    extract(epoch from r.dt_stay_avg) AS dt_stay_avg_seconds,
    r.n_sightings,
    st_length(e.geometry) / date_part('epoch'::text, r.dt_stay_avg) AS v_avg_mps
  FROM results r
    LEFT JOIN {{network_schema}}.road_edges e ON r.from_node = e.from_node AND r.to_node = e.to_node OR r.to_node = e.from_node AND r.from_node = e.to_node
  ORDER BY r.from_node, r.to_node;

comment on materialized view {{vehicle_schema}}.map_static_stats_by_edge is 'Average speed and number of passages per edge (materialized view to speed up MVT generation)';

-- spatial index to speed up MVT generation
CREATE INDEX idx_map_static_stats_by_edge
    ON {{vehicle_schema}}.map_static_stats_by_edge USING gist
    (geometry);

-- serve view as MVT
DROP FUNCTION if exists {{vehicle_schema}}.table_to_mvt(integer, integer, integer);
CREATE OR REPLACE FUNCTION {{vehicle_schema}}.table_to_mvt(zoom integer, x integer, y integer)
RETURNS TABLE(data bytea) 
LANGUAGE 'sql'
AS $BODY$

with bound as (
  select ST_TileEnvelope(zoom, x, y) as bbox
)
select ST_AsMVT(data.*) from (
  select 
    e.from_node,
	  e.to_node,
		ST_AsMVTGeom(st_simplifypreservetopology(st_transform(geometry, 3857), 82000 / 2^(zoom+11)), b.bbox) as geom,
    e.v_avg_mps,
		e."n_sightings",
		e."dt_stay_avg_seconds"
  from 
    {{vehicle_schema}}.map_static_stats_by_edge e,
    bound b
  where st_intersects(st_transform(b.bbox, 2169), geometry)
) as data
$BODY$;

drop table if exists {{vehicle_schema}}.data_files;
create table {{vehicle_schema}}.data_files (
  id uuid NOT NULL primary key,
	filename varchar,
  imported_on timestamp without time zone,
  checksum bytea unique
);
comment on table {{vehicle_schema}}.data_files is 'Data files imported into the system.';
comment on column {{vehicle_schema}}.data_files.id is 'UUID identifying the file locally.';
comment on column {{vehicle_schema}}.data_files.filename is 'Name of the file read in, for convenience (not used for any checks).';
comment on column {{vehicle_schema}}.data_files.imported_on is 'Date and time the import of the file was successfully completed.';
comment on column {{vehicle_schema}}.data_files.checksum is 'SHA256 checksum of the (uncompressed) input data file - used to identify the file.';

drop table if exists {{vehicle_schema}}.data_file_timeframes;
create table {{vehicle_schema}}.data_file_timeframes (
  id uuid primary key,
  file_id uuid references {{vehicle_schema}}.data_files(id),
	vehicle_id int references {{vehicle_schema}}.vehicles(id),
  time_start timestamp without time zone,
  time_end timestamp without time zone,
  unique (file_id, vehicle_id)
);
comment on table {{vehicle_schema}}.data_file_timeframes is 'Vehicle timeframes (periods during which vehicles are active, i.e. have runs and pings) occuring in a data file.';
comment on column {{vehicle_schema}}.data_file_timeframes.time_start is 'earliest `time_start` of the `runs` for every `vehicle` occuring in the file.';
comment on column {{vehicle_schema}}.data_file_timeframes.time_end is 'latest `time_end` of the `runs` for every `vehicle` occuring in the file.';


create type {{vehicle_schema}}.process_log_status 
as ENUM ('complete', 'incomplete', 'failed', 'unknown');

drop table if exists {{vehicle_schema}}.process_log;
create table {{vehicle_schema}}.process_log (
  id uuid primary key,
  timeframe_id uuid references {{vehicle_schema}}.data_file_timeframes(id),
  status {{vehicle_schema}}.process_log_status,
  log text
);
comment on table {{vehicle_schema}}.process_log is 'Describes how processing of the imported behicle timeframes went. There are 4 options: `unkown` means that processing begun, but no data was yet written back, presumably because computations are still ongoing. `complete` is obvious. `failed` means that computations were aborted and no data were written. `incomplete` means that results were written but not all pings were evaluated.';


drop function if exists {{vehicle_schema}}.get_unprocessed_vehicle_timeframes;
create or replace function {{vehicle_schema}}.get_unprocessed_vehicle_timeframes(n int default 1)
returns table (id uuid, vehicle_id int, time_start timestamp, time_end timestamp)
language sql
as $$
select t.id, t.vehicle_id, t.time_start, t.time_end
from {{vehicle_schema}}.data_file_timeframes t
left join {{vehicle_schema}}.process_log l on l.timeframe_id = t.id
where l.status is null
group by t.id
limit n
$$;
comment on function {{vehicle_schema}}.get_unprocessed_vehicle_timeframes is 'Returns the `n` first entries of `data_file_timeframes` that do not yet have any corresponding entry in  `process_log` (including `unknown`)';

drop function if exists {{vehicle_schema}}.get_timeframes_in_file;
create or replace function {{vehicle_schema}}.get_timeframes_in_file(file_id uuid)
returns table (timeframe_id uuid)
language sql
as $$
select t.id as timeframe_id
from {{vehicle_schema}}.data_file_timeframes t
where t.file_id = file_id
$$;
comment on function {{vehicle_schema}}.get_timeframes_in_file is 'Return all timeframe-ids corresponding to given timeframe id';


create materialized view if not exists {{vehicle_schema}}.data_stats as
select * from (
	select id as file_id, filename, imported_on, checksum from {{vehicle_schema}}.data_files
) as d
natural left join (
	select tf.file_id, count(*) as ping_count
	from {{vehicle_schema}}.data_file_timeframes tf 
	left join {{vehicle_schema}}.pings p on p.vehicle_id = tf.vehicle_id and p.time between tf.time_start and tf.time_end
	group by tf.file_id
) as ping_info
natural left join (
	select 
		tf.file_id,
		count(*) as halt_count,
		count(count_people_boarding) as counted_halts,
		sum(count_people_boarding) as total_people_boarding,
		sum(count_people_disembarking) as total_people_disembarking
	from {{vehicle_schema}}.data_file_timeframes tf 
	left join {{vehicle_schema}}.pings_from_stops p on p.vehicle_id = tf.vehicle_id and p.time between tf.time_start and tf.time_end
	group by tf.file_id
) as halt_info
natural left join (
	select
		tf.file_id, count(*) as result_count,
		min(t_enter) as earliest_result,
		max(t_enter + dt_travers) as latest_result
	from {{vehicle_schema}}.data_file_timeframes tf 
	left join {{vehicle_schema}}.results r on r.vehicle_id = tf.vehicle_id and r.t_enter >= tf.time_start and  r.t_enter + r.dt_travers <= tf.time_end
	group by tf.file_id
) as result_info;

comment on materialized view {{vehicle_schema}}.data_stats is 'An extension of the `data_files` table, providing summary information about the actual data and derived results stored in the db.';
comment on column {{vehicle_schema}}.data_stats.ping_count is 'Number of ordinary pings (TYP=-1 - location update messages) imported.';
comment on column {{vehicle_schema}}.data_stats.halt_count is 'Number of halts performed as indicated by pings from stops (TYP!=-1 and TYP!=3).';
comment on column {{vehicle_schema}}.data_stats.counted_halts is 'Number of halts with counting data available. The ratio of `counted_halts` and `halt_count` can be used to extrapolate the total number of passengers.';
comment on column {{vehicle_schema}}.data_stats.total_people_boarding is 'As the name suggests, all boardings reported by all vehicles equiped to do so within the file. Can be extrapolated by multiplying with the ratio of `halt_count` to `counted_halts`.';
comment on column {{vehicle_schema}}.data_stats.total_people_disembarking is 'As the name suggests, sum of all disembarkments reported by all vehicles equiped to do so within the file. Can be extrapolated by multiplying with the ratio of `halt_count` to `counted_halts`.';
comment on column {{vehicle_schema}}.data_stats.result_count is 'Number of road links mapped to the pings contained within this file.';
comment on column {{vehicle_schema}}.data_stats.earliest_result is 'Earliest result entry found matching this file (useful to check for problems with mapmatching)';
comment on column {{vehicle_schema}}.data_stats.latest_result is 'Latest result entry found matching this file (useful to check for problems with mapmatching)';
