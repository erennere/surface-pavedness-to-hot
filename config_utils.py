"""Shared utilities for loading and validating repository configuration."""

import json
import os
import re


DEFAULT_CONFIG_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config.json")
REFERENCE_PATTERN = re.compile(r"\$\{([^}]+)\}")


def resolve_path(path_value, config_path):
    """Resolve configured paths into normalized absolute paths."""
    expanded = os.path.expanduser(path_value)
    if os.path.isabs(expanded):
        return os.path.abspath(expanded)
    config_dir = os.path.dirname(os.path.abspath(config_path))
    return os.path.abspath(os.path.join(config_dir, expanded))


def _get_by_dotted_key(data, dotted_key):
    """Read nested dict value using dotted key notation."""
    current = data
    for key in dotted_key.split("."):
        if not isinstance(current, dict) or key not in current:
            raise KeyError(f"Missing config reference: {dotted_key}")
        current = current[key]
    return current


def _set_by_dotted_key(data, dotted_key, value):
    """Set nested dict value using dotted key notation."""
    keys = dotted_key.split(".")
    current = data
    for key in keys[:-1]:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value


def _resolve_template(value, data):
    """Resolve ${...} references in string values from the same config object."""
    if not isinstance(value, str):
        return value

    def replacer(match):
        ref_key = match.group(1).strip()
        ref_value = _get_by_dotted_key(data, ref_key)
        if not isinstance(ref_value, str):
            raise TypeError(f"Config reference '{ref_key}' must resolve to a string")
        return ref_value

    result = value
    for _ in range(20):
        updated = REFERENCE_PATTERN.sub(replacer, result)
        if updated == result:
            return updated
        result = updated
    raise ValueError(f"Could not fully resolve config template: {value}")


def load_config(config_path):
    """Load JSON config, resolve references, and normalize absolute path entries."""
    with open(config_path, "r", encoding="utf-8") as handle:
        config = json.load(handle)

    flat_string_entries = []

    def collect_strings(node, prefix=""):
        if isinstance(node, dict):
            for key, value in node.items():
                child_prefix = f"{prefix}.{key}" if prefix else key
                collect_strings(value, child_prefix)
        elif isinstance(node, str):
            flat_string_entries.append(prefix)

    collect_strings(config)

    for _ in range(10):
        changed = False
        for dotted_key in flat_string_entries:
            current_value = _get_by_dotted_key(config, dotted_key)
            resolved_value = _resolve_template(current_value, config)
            if resolved_value != current_value:
                _set_by_dotted_key(config, dotted_key, resolved_value)
                changed = True
        if not changed:
            break

    paths_section = config.get("paths", {})

    def absolutize_paths(node):
        if isinstance(node, dict):
            for key, value in node.items():
                node[key] = absolutize_paths(value)
            return node
        if isinstance(node, str):
            return resolve_path(node, config_path)
        return node

    config["paths"] = absolutize_paths(paths_section)
    return config


def get_section(config, section_name):
    """Get a required top-level config section by name."""
    if section_name not in config:
        raise KeyError(f"Missing required config section: {section_name}")
    return config[section_name]


def get_path(config, path_key):
    """Get a required absolute path from config by dotted key.

    If path_key has no dot, it is interpreted as a key under config.paths for
    backward compatibility.
    """
    dotted_key = path_key if "." in path_key else f"paths.{path_key}"
    value = _get_by_dotted_key(config, dotted_key)
    if not isinstance(value, str):
        raise TypeError(f"Config path '{dotted_key}' must resolve to a string")
    return value
