create table {{vehdata_schema}}."results" (
    vehicle_id integer references {{vehdata_schema}}."vehicles"(id),
    from_node integer references {{network_schema}}."road_nodes"(id),
    to_node integer references {{network_schema}}."road_nodes"(id),
    t_enter timestamp,
    dt_travers interval,
    primary key (vehicle_id, t_enter)
);
comment on table {{vehdata_schema}}."results" is 'Trajectories reprojected onto a road-edge basis. Each row describes a vehicle passing over the specified link at and within a given time.';
comment on column {{vehdata_schema}}."results"."vehicle_id" is 'The identity of the vehicle as objserved.';
comment on column {{vehdata_schema}}."results"."from_node" is 'Node from which the vehicle entered the edge.';
comment on column {{vehdata_schema}}."results"."to_node" is 'Node at which the vehicle exited the edge (or at least the direction into which it drove).';
comment on column {{vehdata_schema}}."results"."t_enter" is 'Absolute time of the entry at `from_edge`.';
comment on column {{vehdata_schema}}."results"."dt_travers" is 'Time spent on the link, including with the vehicle at rest.';


create view {{vehdata_schema}}.results_with_runs
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
   FROM {{vehdata_schema}}.results d
     JOIN {{vehdata_schema}}.runs r ON d.t_enter >= r.time_start AND d.t_enter <= r.time_end AND d.vehicle_id = r.vehicle_id
  ORDER BY d.vehicle_id, d.t_enter;

comment on view {{vehdata_schema}}.results_with_runs is 'Flattened representation of `{{vehdata_schema}}.results` table, i.e. including all meta data akin to the input CSV files. Each line represents one passage of a vehicle over a road link.';


create view {{vehdata_schema}}.results_with_runs_and_geometries
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
   FROM {{vehdata_schema}}.results_with_runs r
     LEFT JOIN {{network_schema}}.road_edges e ON e.from_node = r.from_node AND e.to_node = r.to_node OR e.to_node = r.from_node AND e.from_node = r.to_node
     LEFT JOIN {{vehdata_schema}}.lines l ON r.line_id = l.id;

comment on view {{vehdata_schema}}.results_with_runs_and_geometries is 'Same as `{{vehdata_schema}}.results_with_runs` (flattened representation of `{{vehdata_schema}}.results` table, including all meta-data) but including the actual geometry instead of just its `from_node` and `to_node`. Note: the `road_forwards` field indicates the direction of travel (relative to the geometry) instead of the `from_node` and `to_node` fields.';


-- View: bus_data.map_static_stats_by_edge
CREATE MATERIALIZED VIEW {{vehdata_schema}}.map_static_stats_by_edge
AS
  WITH results AS (
    SELECT results.from_node,
      results.to_node,
      avg(results.dt_travers) AS dt_stay_avg,
      count(*) AS n_sightings
      FROM {{vehdata_schema}}.results
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

comment on materialized view {{vehdata_schema}}.map_static_stats_by_edge is 'Average speed and number of passages per edge (materialized view to speed up MVT generation)';

-- spatial index to speed up MVT generation
CREATE INDEX idx_map_static_stats_by_edge
    ON {{vehdata_schema}}.map_static_stats_by_edge USING gist
    (geometry);

-- serve view as MVT
create function {{vehdata_schema}}.table_to_mvt(zoom integer, x integer, y integer)
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
    {{vehdata_schema}}.map_static_stats_by_edge e,
    bound b
  where st_intersects(st_transform(b.bbox, 2169), geometry)
) as data
$BODY$;


create materialized view {{vehdata_schema}}.data_stats as
select * from (
	select id as file_id, filename, imported_on, checksum from {{vehdata_schema}}.data_files
) as d
natural left join (
	select tf.file_id, count(*) as ping_count
	from {{vehdata_schema}}.data_file_timeframes tf 
	left join {{vehdata_schema}}.pings p on p.vehicle_id = tf.vehicle_id and p.time between tf.time_start and tf.time_end
	group by tf.file_id
) as ping_info
natural left join (
	select 
		tf.file_id,
		count(*) as halt_count,
		count(count_people_boarding) as counted_halts,
		sum(count_people_boarding) as total_people_boarding,
		sum(count_people_disembarking) as total_people_disembarking
	from {{vehdata_schema}}.data_file_timeframes tf 
	left join {{vehdata_schema}}.pings_from_stops p on p.vehicle_id = tf.vehicle_id and p.time between tf.time_start and tf.time_end
	group by tf.file_id
) as halt_info
natural left join (
	select
		tf.file_id, count(*) as result_count,
		min(t_enter) as earliest_result,
		max(t_enter + dt_travers) as latest_result
	from {{vehdata_schema}}.data_file_timeframes tf 
	left join {{vehdata_schema}}.results r on r.vehicle_id = tf.vehicle_id and r.t_enter >= tf.time_start and  r.t_enter + r.dt_travers <= tf.time_end
	group by tf.file_id
) as result_info;

comment on materialized view {{vehdata_schema}}.data_stats is 'An extension of the `data_files` table, providing summary information about the actual data and derived results stored in the db.';
comment on column {{vehdata_schema}}.data_stats.ping_count is 'Number of ordinary pings (TYP=-1 - location update messages) imported.';
comment on column {{vehdata_schema}}.data_stats.halt_count is 'Number of halts performed as indicated by pings from stops (TYP!=-1 and TYP!=3).';
comment on column {{vehdata_schema}}.data_stats.counted_halts is 'Number of halts with counting data available. The ratio of `counted_halts` and `halt_count` can be used to extrapolate the total number of passengers.';
comment on column {{vehdata_schema}}.data_stats.total_people_boarding is 'As the name suggests, all boardings reported by all vehicles equiped to do so within the file. Can be extrapolated by multiplying with the ratio of `halt_count` to `counted_halts`.';
comment on column {{vehdata_schema}}.data_stats.total_people_disembarking is 'As the name suggests, sum of all disembarkments reported by all vehicles equiped to do so within the file. Can be extrapolated by multiplying with the ratio of `halt_count` to `counted_halts`.';
comment on column {{vehdata_schema}}.data_stats.result_count is 'Number of road links mapped to the pings contained within this file.';
comment on column {{vehdata_schema}}.data_stats.earliest_result is 'Earliest result entry found matching this file (useful to check for problems with mapmatching)';
comment on column {{vehdata_schema}}.data_stats.latest_result is 'Latest result entry found matching this file (useful to check for problems with mapmatching)';