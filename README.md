# surface-pavedness-to-hot

Repository for downloading HOTOSM road exports, enriching them with road-surface predictions, and generating country-level statistics.

## Shared Configuration

All scripts read from one config file: [config.json](config.json)

Run any script with:

```bash
python <script_name>.py --config /absolute/path/to/config.json
```

All configured paths are resolved to absolute paths at runtime by [config_utils.py](config_utils.py).

## Scripts

### get_hot.py

File: [get_hot.py](get_hot.py)

Purpose:
1. Search HDX for HOTOSM roads datasets.
2. Select latest resource per country.
3. Download and extract files.
4. Write country metadata JSON.

Key config sections:
1. `paths.hotosm.download_dir`
2. `paths.hotosm.metadata_dir`
3. `get_hot`

### merge_hotosm.py

File: [merge_hotosm.py](merge_hotosm.py)

Purpose:
1. Load HOTOSM files from download directory.
2. Join with prediction rows by `osm_id`.
3. Export merged GeoJSON and GPKG per country.
4. Emit coverage comparison CSVs.

Key config sections:
1. `paths.hotosm.download_dir`
2. `paths.predictions.root_dir`
3. `paths.hotosm.updated_dir`
4. `merge_hotosm`

### create_stats.py

File: [create_stats.py](create_stats.py)

Purpose:
1. Read merged country files.
2. Compute aggregate road-length statistics in kilometers.
3. Write per-country stats CSV.

Output metrics:
1. `total_road_length`
2. `total_unpaved_length`
3. `total_paved_length`
4. `total_missing_length`
5. `provided_length`

Key config sections:
1. `paths.hotosm.new_countries_dir`
2. `create_stats`

### compare_hot_our_data.py

File: [compare_hot_our_data.py](compare_hot_our_data.py)

Purpose:
1. Compare HOTOSM country stats with available prediction-country folders.
2. Export CSV lists for missing/empty coverage cases.

Key config sections:
1. `paths.hotosm.country_stats_file`
2. `paths.predictions.final_partitioned_filtered_dir`
3. `paths.hotosm.comparison_dir`
4. `compare_hot_our_data`

### other_countries.py

File: [other_countries.py](other_countries.py)

Purpose:
1. Create HOT-format files for configured country exceptions.
2. Merge prediction rows with source OSM tags.
3. Export GeoJSON and GPKG outputs.

Key config sections:
1. `paths.source_osm.original_osm_dir`
2. `paths.predictions.root_dir`
3. `paths.hotosm.root_dir`
4. `other_countries`

### other_countries_usa.py

File: [other_countries_usa.py](other_countries_usa.py)

Purpose:
1. Same workflow as `other_countries.py` but with USA-focused country mapping.

Key config sections:
1. `paths.source_osm.original_osm_dir`
2. `paths.predictions.root_dir`
3. `paths.hotosm.root_dir`
4. `other_countries_usa`






