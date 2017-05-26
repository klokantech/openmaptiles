DO $$
BEGIN
  update osm_continent_point SET tags = slice_language_tags(tags) || get_basic_names(tags);
  update osm_country_point SET tags = slice_language_tags(tags) || get_basic_names(tags);
  update osm_island_polygon SET tags = slice_language_tags(tags) || get_basic_names(tags);
  update osm_island_point SET tags = slice_language_tags(tags) || get_basic_names(tags);
  update osm_state_point SET tags = slice_language_tags(tags) || get_basic_names(tags);
  update osm_city_point SET tags = slice_language_tags(tags) || get_basic_names(tags);
END $$;
