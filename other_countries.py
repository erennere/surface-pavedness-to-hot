"""Create HOT-format files for specific countries missing from HOTOSM coverage."""

import argparse
import logging
import os
import shutil

import duckdb
import geopandas as gpd
from shapely import from_wkt

from config_utils import DEFAULT_CONFIG_PATH, get_path, get_section, load_config


LOGGER = logging.getLogger(__name__)
COLS_FROM_ORG = [
    "osm_id",
    "osm_type",
    "name",
    "smoothness",
    "width",
    "lanes",
    "oneway",
    "bridge",
    "layer",
    "source",
]


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Generate missing-country HOT files.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to shared config.json")
    return parser.parse_args()


def find_rows(ids, temp_dir, osm_filepath, index):
    """Find OSM tag rows for a given ID list."""
    temp_file = os.path.abspath(os.path.join(temp_dir, f"temp_{index}.db"))
    conn = duckdb.connect(temp_file)
    try:
        conn.register("ids", ids)
        query = f"""
            SELECT
                osm_id,
                osm_type,
                list_extract(map_extract(tags, 'name'), 1) AS name,
                list_extract(map_extract(tags, 'smoothness'), 1) AS smoothness,
                list_extract(map_extract(tags, 'width'), 1) AS width,
                list_extract(map_extract(tags, 'lanes'), 1) AS lanes,
                list_extract(map_extract(tags, 'oneway'), 1) AS oneway,
                list_extract(map_extract(tags, 'bridge'), 1) AS bridge,
                list_extract(map_extract(tags, 'layer'), 1) AS layer,
                list_extract(map_extract(tags, 'source'), 1) AS source
            FROM read_parquet('{osm_filepath}/*.parquet')
            WHERE osm_id IN (SELECT osm_id FROM ids)
        """
        return conn.sql(query).df()
    finally:
        conn.close()
        if os.path.exists(temp_file):
            os.remove(temp_file)


def process_country(country, predictions_dir, temp_dir, index):
    """Load prediction rows for a country from parquet partitions."""
    temp_file = os.path.abspath(os.path.join(temp_dir, f"temp_country_{index}.db"))
    conn = duckdb.connect(temp_file)
    try:
        query = f"""
        SELECT
            continent,
            country AS country_iso_a2,
            country_iso_a3,
            urban,
            urban_area,
            osm_id AS osm_id_original,
            TRY_CAST(regexp_extract(osm_id, '.*/(\\d+)$', 1) AS BIGINT) AS osm_id,
            osm_tags_highway,
            osm_tags_surface,
            osm_surface_class,
            osm_surface,
            pred_class,
            pred_label,
            combined_surface_osm_priority,
            combined_surface_DL_priority,
            changeset_timestamp AS osm_changeset_timestamp,
            mean_timestamp AS dl_mean_timestamp,
            length AS osm_length,
            predicted_length,
            n_of_predictions_used,
            geometry
        FROM read_parquet('{predictions_dir}/predictions_updated_*.parquet')
        WHERE country = '{country}'
        """
        return conn.sql(query).df()
    finally:
        conn.close()
        if os.path.exists(temp_file):
            os.remove(temp_file)


def merge(countries, predictions_dir, original_osm_dir, temp_dir, output_dir, output_subdir):
    """Merge OSM base rows with prediction rows for configured countries."""
    output_dir = os.path.abspath(os.path.join(output_dir, output_subdir))
    os.makedirs(output_dir, exist_ok=True)

    for index, country in enumerate(countries.keys()):
        predictions = process_country(country, predictions_dir, temp_dir, index)
        if predictions is None or predictions.empty:
            LOGGER.warning("No prediction rows for %s", country)
            continue

        ids = predictions["osm_id"].to_frame()
        ids["osm_id"] = ids["osm_id"].apply(lambda value: f"way/{value}")
        osm_data = find_rows(ids, temp_dir, original_osm_dir, index)

        temp_file = os.path.abspath(os.path.join(temp_dir, f"temp_join_{index}.db"))
        conn = duckdb.connect(temp_file)
        try:
            conn.execute("LOAD SPATIAL;")
            conn.register("predictions", predictions)
            conn.register("osm_data", osm_data)

            query = """
            SELECT
                b.continent,
                b.country_iso_a2,
                b.country_iso_a3,
                b.urban,
                b.urban_area,
                b.osm_id,
                a.osm_type,
                b.osm_tags_highway AS highway,
                b.osm_tags_surface AS surface,
                a.smoothness,
                b.osm_surface_class,
                b.pred_class,
                b.pred_label,
                b.combined_surface_osm_priority,
                b.combined_surface_DL_priority,
                b.osm_changeset_timestamp,
                b.dl_mean_timestamp,
                b.osm_length,
                b.predicted_length,
                b.n_of_predictions_used,
                a.name,
                a.width,
                a.lanes,
                a.oneway,
                a.bridge,
                a.layer,
                a.source,
                b.geometry
            FROM predictions b
            LEFT JOIN osm_data a
                ON b.osm_id_original = a.osm_id
            """

            result = conn.sql(query).df()
            result["geometry"] = result["geometry"].apply(lambda geom: from_wkt(geom))
            result = gpd.GeoDataFrame(result, geometry="geometry", crs="EPSG:4326")

            form = f"heigit_{countries[country].lower()}_roadsurface_lines"
            result.to_file(os.path.abspath(os.path.join(output_dir, f"{form}.geojson")), driver="GeoJSON", index=False)
            result.to_file(os.path.abspath(os.path.join(output_dir, f"{form}.gpkg")), driver="GPKG", index=False)
            LOGGER.info("Created output files for %s", country)
        finally:
            conn.close()
            if os.path.exists(temp_file):
                os.remove(temp_file)


def main(config_path):
    """Run workflow for creating files for missing countries."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    config = load_config(config_path)
    script_config = get_section(config, "other_countries")

    original_osm_dir = get_path(config, "paths.source_osm.original_osm_dir")
    predictions_dir = get_path(config, "paths.predictions.root_dir")
    output_dir = get_path(config, "paths.hotosm.root_dir")
    temp_dir = os.path.abspath(os.path.join(output_dir, "temp"))
    os.makedirs(temp_dir, exist_ok=True)

    merge(
        script_config["countries"],
        predictions_dir,
        original_osm_dir,
        temp_dir,
        output_dir,
        script_config.get("output_subdir", "new_countries"),
    )

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


if __name__ == "__main__":
    ARGS = parse_args()
    main(ARGS.config)
