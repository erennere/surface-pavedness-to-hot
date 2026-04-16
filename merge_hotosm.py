"""Merge HOTOSM road files with model predictions using OSM IDs."""

import argparse
import logging
import os
import random
import re
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed

import duckdb
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import from_wkt, to_wkt

from config_utils import DEFAULT_CONFIG_PATH, get_path, get_section, load_config


LOGGER = logging.getLogger(__name__)
SUPPORTED_EXTENSIONS = (".geojson", ".shp", ".gpkg", ".kml")


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Merge HOTOSM files with prediction metadata.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to shared config.json")
    return parser.parse_args()


def extract_first_wildcard(test_string, pattern):
    """Extract a country code from the HOTOSM file name."""
    match = re.search(pattern, test_string)
    if match:
        return match.group(1)
    return None


def create_and_zip(result, filepath, driver):
    """Write a geospatial file and zip it for downstream use."""
    try:
        result.to_file(filepath, driver=driver, index=False)
        with zipfile.ZipFile(f"{filepath}.zip", "w") as zipf:
            zipf.write(filepath, os.path.basename(filepath))
        os.remove(filepath)
    except Exception as error:  # pylint: disable=broad-exception-caught
        LOGGER.exception("Failed to write and zip %s: %s", filepath, error)


def find_rows(country_iso3, pred_pattern, memory_gb, threads):
    """Load rows for one country from the prediction parquet files."""
    query = f"""
    SELECT
        continent,
        country AS country_iso_a2,
        country_iso_a3,
        urban,
        urban_area,
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
        mean_timestamp AS DL_mean_timestamp,
        length AS osm_length,
        predicted_length,
        n_of_predictions_used
    FROM read_parquet('{pred_pattern}')
    WHERE regexp_matches(UPPER(CAST(country_iso_a3 AS VARCHAR)), '(^|[^A-Z]){country_iso3}([^A-Z]|$)')
    """

    conn = duckdb.connect(":memory:")
    try:
        conn.execute(f"SET memory_limit='{memory_gb}GB';")
        conn.execute(f"SET threads TO {threads};")
        return conn.execute(query).df()
    finally:
        conn.close()


def build_merge_query(hotosm_columns):
    """Build SQL query that merges HOTOSM rows with prediction rows."""
    rest_columns = {"highway", "surface", "smoothness", "osm_id", "osm_type", "geometry"}
    cols_to_be_parsed = ", ".join([f"a.{col}" for col in hotosm_columns if col not in rest_columns])

    return f"""
    SELECT
        b.continent,
        b.country_iso_a2,
        b.country_iso_a3,
        b.urban,
        b.urban_area,
        b.osm_id,
        a.osm_type,
        CASE WHEN b.osm_tags_highway IS NOT NULL THEN b.osm_tags_highway ELSE a.highway END AS highway,
        CASE WHEN b.osm_tags_surface IS NOT NULL THEN b.osm_tags_surface ELSE a.surface END AS surface,
        a.smoothness,
        CASE
            WHEN b.osm_surface_class IS NOT NULL THEN b.osm_surface_class
            WHEN list_contains(['paved', 'asphalt', 'chipseal', 'concrete', 'concrete:lanes', 'concrete:plates', 'paving_stones', 'sett', 'unhewn_cobblestone', 'cobblestone', 'bricks', 'metal', 'wood'], a.surface) THEN 'paved'
            WHEN list_contains(['unpaved', 'compacted', 'fine_gravel', 'gravel', 'shells', 'rock', 'pebblestone', 'ground', 'dirt', 'earth', 'grass', 'grass_paver', 'metal_grid', 'mud', 'sand', 'woodchips', 'snow', 'ice', 'salt'], a.surface) THEN 'unpaved'
            ELSE NULL
        END AS osm_surface_class,
        b.pred_class,
        b.pred_label,
        CASE
            WHEN b.combined_surface_osm_priority IS NOT NULL THEN b.combined_surface_osm_priority
            WHEN b.osm_surface_class IS NOT NULL THEN b.osm_surface_class
            WHEN list_contains(['paved', 'asphalt', 'chipseal', 'concrete', 'concrete:lanes', 'concrete:plates', 'paving_stones', 'sett', 'unhewn_cobblestone', 'cobblestone', 'bricks', 'metal', 'wood'], a.surface) THEN 'paved'
            WHEN list_contains(['unpaved', 'compacted', 'fine_gravel', 'gravel', 'shells', 'rock', 'pebblestone', 'ground', 'dirt', 'earth', 'grass', 'grass_paver', 'metal_grid', 'mud', 'sand', 'woodchips', 'snow', 'ice', 'salt'], a.surface) THEN 'unpaved'
            ELSE b.pred_class
        END AS combined_surface_osm_priority,
        CASE
            WHEN b.pred_class IS NOT NULL THEN b.pred_class
            WHEN b.osm_surface_class IS NOT NULL THEN b.osm_surface_class
            WHEN list_contains(['paved', 'asphalt', 'chipseal', 'concrete', 'concrete:lanes', 'concrete:plates', 'paving_stones', 'sett', 'unhewn_cobblestone', 'cobblestone', 'bricks', 'metal', 'wood'], a.surface) THEN 'paved'
            WHEN list_contains(['unpaved', 'compacted', 'fine_gravel', 'gravel', 'shells', 'rock', 'pebblestone', 'ground', 'dirt', 'earth', 'grass', 'grass_paver', 'metal_grid', 'mud', 'sand', 'woodchips', 'snow', 'ice', 'salt'], a.surface) THEN 'unpaved'
            ELSE NULL
        END AS combined_surface_DL_priority,
        b.osm_changeset_timestamp,
        b.DL_mean_timestamp,
        b.osm_length,
        b.predicted_length,
        b.n_of_predictions_used,
        {cols_to_be_parsed},
        a.geometry
    FROM hotosm_data a
    LEFT JOIN our_data b
        ON TRY_CAST(a.osm_id AS BIGINT) = TRY_CAST(b.osm_id AS BIGINT)
    """


def merge_file(hotosm_filepath, predictions_dir, output_dir, pattern, memory_gb, threads):
    """Merge one HOTOSM file with available prediction rows."""
    country = extract_first_wildcard(os.path.basename(hotosm_filepath), pattern)
    if not country:
        LOGGER.warning("Could not extract country from %s", hotosm_filepath)
        return {"country": None, "in_our_data": False}

    country = country.upper()
    hotosm_data = gpd.read_file(hotosm_filepath)
    crs = hotosm_data.crs

    columns_with_colon = [col for col in hotosm_data.columns if ":" in col]
    normalized_columns = {col: col.replace(":", "_") for col in hotosm_data.columns}
    hotosm_data.rename(normalized_columns, axis=1, inplace=True)

    pred_pattern = "predictions_updated_*.parquet"
    pred_pattern = os.path.join(predictions_dir, pred_pattern)
    our_data = find_rows(country, pred_pattern, memory_gb, threads)
    has_our_rows = our_data is not None and not our_data.empty

    final_columns = [
        "continent",
        "country_iso_a2",
        "country_iso_a3",
        "urban",
        "urban_area",
        "osm_id",
        "osm_type",
        "highway",
        "surface",
        "smoothness",
        "osm_surface_class",
        "pred_class",
        "pred_label",
        "combined_surface_osm_priority",
        "combined_surface_DL_priority",
        "osm_changeset_timestamp",
        "DL_mean_timestamp",
        "osm_length",
        "predicted_length",
        "n_of_predictions_used",
    ]
    excluded = {"highway", "surface", "smoothness", "osm_id", "osm_type", "geometry"}
    final_columns.extend([col for col in hotosm_data.columns if col not in excluded])

    if has_our_rows:
        hotosm_data["geometry"] = hotosm_data["geometry"].apply(lambda geom: to_wkt(geom))
        query = build_merge_query(hotosm_data.columns)

        conn = duckdb.connect(":memory:")
        try:
            conn.execute(f"SET memory_limit='{memory_gb}GB';")
            conn.execute(f"SET threads TO {threads};")
            conn.register("hotosm_data", hotosm_data)
            conn.register("our_data", our_data)
            result = conn.execute(query).df()
        finally:
            conn.close()

        result["geometry"] = result["geometry"].apply(lambda geom: from_wkt(geom))
    else:
        for column in final_columns:
            if column not in hotosm_data.columns:
                hotosm_data[column] = np.nan
        result = hotosm_data[final_columns]

    geo_result = gpd.GeoDataFrame(result, geometry="geometry", crs=crs)

    restore_columns = {
        col: col.replace("_", ":")
        for col in geo_result.columns
        if col.replace("_", ":") in columns_with_colon
    }
    geo_result.rename(restore_columns, axis=1, inplace=True)

    form = f"heigit_{country.lower()}_roadsurface_lines"
    gpkg_path = os.path.abspath(os.path.join(output_dir, f"{form}.gpkg"))
    geojson_path = os.path.abspath(os.path.join(output_dir, f"{form}.geojson"))

    create_and_zip(geo_result, gpkg_path, "GPKG")
    create_and_zip(geo_result, geojson_path, "GeoJSON")

    LOGGER.info("Merged country %s from %s", country, hotosm_filepath)
    return {"country": country, "in_our_data": has_our_rows}


def run(config_path):
    """Execute the HOTOSM merge workflow."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    config = load_config(config_path)
    script_config = get_section(config, "merge_hotosm")

    path_to_hotosm = get_path(config, "paths.hotosm.download_dir")
    path_to_predictions = get_path(config, "paths.predictions.root_dir")
    path_to_output = get_path(config, "paths.hotosm.updated_dir")

    os.makedirs(path_to_output, exist_ok=True)

    hotosm_files = [
        os.path.abspath(os.path.join(path_to_hotosm, filename))
        for filename in os.listdir(path_to_hotosm)
        if filename.startswith("hotosm_") and filename.lower().endswith(SUPPORTED_EXTENSIONS)
    ]

    if bool(script_config.get("shuffle_inputs", True)):
        random.shuffle(hotosm_files)

    workers = int(script_config.get("workers", 1))
    memory_gb = int(script_config.get("memory_gb", 32))
    pattern = script_config["pattern"]
    run_parallel = bool(script_config.get("run_parallel", False))

    statuses = []
    if run_parallel and workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    merge_file,
                    hotosm_filepath,
                    path_to_predictions,
                    path_to_output,
                    pattern,
                    max(1, memory_gb // workers),
                    max(1, (os.cpu_count() or 1) // workers),
                ): hotosm_filepath
                for hotosm_filepath in hotosm_files
            }
            for future in as_completed(futures):
                try:
                    statuses.append(future.result())
                except Exception as error:  # pylint: disable=broad-exception-caught
                    LOGGER.exception("Merge failure for %s: %s", futures[future], error)
    else:
        for hotosm_filepath in hotosm_files:
            try:
                statuses.append(
                    merge_file(
                        hotosm_filepath,
                        path_to_predictions,
                        path_to_output,
                        pattern,
                        memory_gb,
                        os.cpu_count() or 1,
                    )
                )
            except Exception as error:  # pylint: disable=broad-exception-caught
                LOGGER.exception("Merge failure for %s: %s", hotosm_filepath, error)

    countries_in_both = {item["country"] for item in statuses if item.get("country") and item.get("in_our_data")}
    countries_not_in_our = {item["country"] for item in statuses if item.get("country") and not item.get("in_our_data")}

    not_in_hots = duckdb.sql(
        f"""
        SELECT DISTINCT country_iso_a3
        FROM read_parquet('{path_to_predictions}/predictions_updated_*.parquet')
        """
    ).df()
    not_in_hots = not_in_hots[~not_in_hots["country_iso_a3"].isin(countries_in_both)]

    pd.DataFrame(sorted(countries_not_in_our), columns=["country_iso_a3"]).to_csv(
        os.path.abspath(os.path.join(path_to_output, script_config["missing_in_our_filename"])), index=False
    )
    pd.DataFrame(sorted(countries_in_both), columns=["country_iso_a3"]).to_csv(
        os.path.abspath(os.path.join(path_to_output, script_config["in_both_filename"])), index=False
    )
    not_in_hots.to_csv(
        os.path.abspath(os.path.join(path_to_output, script_config["missing_in_hots_filename"])), index=False
    )

    LOGGER.info("Merge pipeline completed for %d HOTOSM files.", len(hotosm_files))


if __name__ == "__main__":
    ARGS = parse_args()
    run(ARGS.config)
