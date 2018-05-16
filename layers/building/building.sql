
--\timing on

--fce, která bude vracet recordset a z tý bude matview
--

CREATE OR REPLACE FUNCTION osm_building_block_gen1( )
RETURNS table(
   osm_id bigint
   , geometry geometry
)
LANGUAGE plpgsql
VOLATILE
 -- common options:  IMMUTABLE  STABLE  STRICT  SECURITY DEFINER
AS $function$
DECLARE
zres14 float := Zres(14);
zres12 float := Zres(12);
BEGIN
   FOR osm_id, geometry IN
      WITH dta AS ( --CTE is used because of optimalization 
	 SELECT o.osm_id, o.geometry
	 , ST_ClusterDBSCAN(o.geometry, eps := zres14, minpoints := 1) over() cid
	 FROM osm_building_polygon o
      ) 
      SELECT 
      (array_agg(dta.osm_id))[1]
      , ST_Buffer(
	 ST_MemUnion(
	    ST_Buffer(
	       dta.geometry
	       , zres14
	       , 'join=mitre'
	    )
	 )
	 , -zres14
	 , 'join=mitre'
      ) geometry
      FROM dta
      GROUP BY cid
      LOOP
	 --removing holes smaller than
	 IF ST_NumInteriorRings(geometry) > 0 THEN--only from geometries wih holes
	    geometry := (
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
		     AND ST_Area(rg.geom) >= power(zres12,2) --4 bigger than
		  ) holes
	       ) new_geom
	    );
	 END IF;

	 IF ST_Area(geometry) < power(zres12,2) THEN
	    CONTINUE;
	 END IF;

	 --simplify
	 geometry := ST_SimplifyPreserveTopology(geometry, zres14::float);

	 RETURN NEXT;
      END LOOP;
END;

$function$;

DROP MATERIALIZED VIEW IF EXISTS osm_building_block_gen1; --drop table, if exists

DROP TABLE IF EXISTS osm_building_block_gen1; --drop table, if exists

DROP TABLE IF EXISTS osm_building_polygon_gen1; --drop table, if exists, clean after previous version of sql script

CREATE MATERIALIZED VIEW osm_building_block_gen1 AS
SELECT * FROM osm_building_block_gen1();

CREATE INDEX on osm_building_block_gen1 USING gist(geometry);
CREATE UNIQUE INDEX on osm_building_block_gen1 USING btree(osm_id);

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
        -- etldoc: osm_building_block_gen1 -> layer_building:z13
        SELECT
            osm_id, geometry,
            NULL::int AS render_height, NULL::int AS render_min_height
        FROM osm_building_block_gen1
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
