--\timing on

/*
 * arbitrary tolerance, i don`t know, how to set zres values to sql script
 * is possible use another values
 */

\set tolerance2 10 --for ST_Simplify, clustering a and buffers for removing "slivers"

\set tolerance1 40 --second power of this is used for removing small polygons and small holes

DROP TABLE IF EXISTS osm_building_polygon_gen1; --drop table, if exists

CREATE TABLE osm_building_polygon_gen1
(
   osm_id bigint primary key
   , geometry geometry(GEOMETRY, 3857)
);


/*
 * grouping buildings on ST_ClusterDBSCAN
 * ST_ClusterDBSCAN is window function for creating groups of geometries closer
 * than some distance
 */

INSERT INTO osm_building_polygon_gen1
WITH dta AS ( --CTE is used because of optimalization 
   SELECT osm_id, geometry
   , ST_ClusterDBSCAN(geometry, eps := :tolerance2, minpoints := 1) over() cid
   FROM osm_building_polygon
) 
SELECT 
(array_agg(osm_id))[1]
, ST_Buffer(
   ST_MemUnion(
      ST_Buffer(
	 geometry
	 , :tolerance2
	 , 'join=mitre'
      )
   )
   , -:tolerance2
   , 'join=mitre'
) geometry
FROM dta
GROUP BY cid;

CREATE INDEX on osm_building_polygon_gen1 USING gist(geometry);

--removing holes smaller than
UPDATE osm_building_polygon_gen1
SET geometry = (
   SELECT ST_Collect( --there are some multigeometries in this layer
      gn
   )
   FROM (
      SELECT
      COALESCE( --in some cases are "holes" NULL, because all holes are smaller than
	 ST_MakePolygon(
	    ST_ExteriorRing(dmp.geom) --exterior ring
	    , holes 
	 )
	 , 
	 ST_MakePolygon(
	    ST_ExteriorRing(dmp.geom)
	 )
      ) gn
      FROM
      ST_Dump(geometry) dmp --1 dump polygons
      , LATERAL (
	 SELECT
	 ST_Accum(
	    ST_Boundary(rg.geom) --2 create array
	 ) holes
	 FROM
	 ST_DumpRings(dmp.geom) rg --3 from rings
	 WHERE rg.path[1] > 0 --5 except inner ring
	 AND ST_Area(rg.geom) >= power(:tolerance1,2) --4 bigger than
      ) holes
   ) new_geom
)
WHERE ST_NumInteriorRings(geometry) > 0; --only from geometries wih holes

--delete small geometries
DELETE FROM osm_building_polygon_gen1
WHERE ST_Area(geometry) < power(:tolerance1,2)
OR NOT ST_IsValid(geometry); --it was in imposm workflow, maybe it shoul better use ST_MakeValid

--simplify
UPDATE osm_building_polygon_gen1
SET geometry = ST_SimplifyPreserveTopology(geometry, :tolerance2::float);


-- etldoc: layer_building[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="layer_building | <z13> z13 | <z14_> z14+ " ] ;

CREATE OR REPLACE FUNCTION as_numeric(text) RETURNS NUMERIC AS $$
 -- Inspired by http://stackoverflow.com/questions/16195986/isnumeric-with-postgresql/16206123#16206123
DECLARE test NUMERIC;
BEGIN
     test = $1::NUMERIC;
     RETURN test;
EXCEPTION WHEN others THEN
     RETURN -1;
END;
$$ STRICT
LANGUAGE plpgsql IMMUTABLE;

CREATE INDEX IF NOT EXISTS osm_building_relation_building_idx ON osm_building_relation(building);
--CREATE INDEX IF NOT EXISTS osm_building_associatedstreet_role_idx ON osm_building_associatedstreet(role);
--CREATE INDEX IF NOT EXISTS osm_building_street_role_idx ON osm_building_street(role);

CREATE OR REPLACE VIEW osm_all_buildings AS (
         -- etldoc: osm_building_relation -> layer_building:z14_
         -- Buildings built from relations
         SELECT member AS osm_id,geometry,
                  COALESCE(nullif(as_numeric(height),-1),nullif(as_numeric(buildingheight),-1)) as height,
                  COALESCE(nullif(as_numeric(min_height),-1),nullif(as_numeric(buildingmin_height),-1)) as min_height,
                  COALESCE(nullif(as_numeric(levels),-1),nullif(as_numeric(buildinglevels),-1)) as levels,
                  COALESCE(nullif(as_numeric(min_level),-1),nullif(as_numeric(buildingmin_level),-1)) as min_level
         FROM
         osm_building_relation WHERE building = ''
         UNION ALL

         -- etldoc: osm_building_associatedstreet -> layer_building:z14_
         -- Buildings in associatedstreet relations
         SELECT member AS osm_id,geometry,
                  COALESCE(nullif(as_numeric(height),-1),nullif(as_numeric(buildingheight),-1)) as height,
                  COALESCE(nullif(as_numeric(min_height),-1),nullif(as_numeric(buildingmin_height),-1)) as min_height,
                  COALESCE(nullif(as_numeric(levels),-1),nullif(as_numeric(buildinglevels),-1)) as levels,
                  COALESCE(nullif(as_numeric(min_level),-1),nullif(as_numeric(buildingmin_level),-1)) as min_level
         FROM
         osm_building_associatedstreet WHERE role = 'house'
         UNION ALL
         -- etldoc: osm_building_street -> layer_building:z14_
         -- Buildings in street relations
         SELECT member AS osm_id,geometry,
                  COALESCE(nullif(as_numeric(height),-1),nullif(as_numeric(buildingheight),-1)) as height,
                  COALESCE(nullif(as_numeric(min_height),-1),nullif(as_numeric(buildingmin_height),-1)) as min_height,
                  COALESCE(nullif(as_numeric(levels),-1),nullif(as_numeric(buildinglevels),-1)) as levels,
                  COALESCE(nullif(as_numeric(min_level),-1),nullif(as_numeric(buildingmin_level),-1)) as min_level
         FROM
         osm_building_street WHERE role = 'house'
         UNION ALL

         -- etldoc: osm_building_multipolygon -> layer_building:z14_
         -- Buildings that are inner/outer
         SELECT osm_id,geometry,
                  COALESCE(nullif(as_numeric(height),-1),nullif(as_numeric(buildingheight),-1)) as height,
                  COALESCE(nullif(as_numeric(min_height),-1),nullif(as_numeric(buildingmin_height),-1)) as min_height,
                  COALESCE(nullif(as_numeric(levels),-1),nullif(as_numeric(buildinglevels),-1)) as levels,
                  COALESCE(nullif(as_numeric(min_level),-1),nullif(as_numeric(buildingmin_level),-1)) as min_level
         FROM
         osm_building_polygon obp WHERE EXISTS (SELECT 1 FROM osm_building_multipolygon obm WHERE obp.osm_id = obm.osm_id)
         UNION ALL
         -- etldoc: osm_building_polygon -> layer_building:z14_
         -- Standalone buildings
         SELECT osm_id,geometry,
                  COALESCE(nullif(as_numeric(height),-1),nullif(as_numeric(buildingheight),-1)) as height,
                  COALESCE(nullif(as_numeric(min_height),-1),nullif(as_numeric(buildingmin_height),-1)) as min_height,
                  COALESCE(nullif(as_numeric(levels),-1),nullif(as_numeric(buildinglevels),-1)) as levels,
                  COALESCE(nullif(as_numeric(min_level),-1),nullif(as_numeric(buildingmin_level),-1)) as min_level
         FROM
         osm_building_polygon WHERE osm_id >= 0
);

CREATE OR REPLACE FUNCTION layer_building(bbox geometry, zoom_level int)
RETURNS TABLE(geometry geometry, osm_id bigint, render_height int, render_min_height int) AS $$
    SELECT geometry, osm_id, render_height, render_min_height
    FROM (
        -- etldoc: osm_building_polygon_gen1 -> layer_building:z13
        SELECT
            osm_id, geometry,
            NULL::int AS render_height, NULL::int AS render_min_height
        FROM osm_building_polygon_gen1
        WHERE zoom_level = 13 AND geometry && bbox
        UNION ALL
        -- etldoc: osm_building_polygon -> layer_building:z14_
        SELECT DISTINCT ON (osm_id)
           osm_id, geometry,
           ceil( COALESCE(height, levels*3.66,5))::int AS render_height,
           floor(COALESCE(min_height, min_level*3.66,0))::int AS render_min_height FROM
        osm_all_buildings
        WHERE
            (levels IS NULL OR levels < 1000) AND
            zoom_level >= 14 AND geometry && bbox
    ) AS zoom_levels
    ORDER BY render_height ASC, ST_YMin(geometry) DESC;
$$ LANGUAGE SQL IMMUTABLE;

-- not handled: where a building outline covers building parts
