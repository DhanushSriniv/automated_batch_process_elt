import requests
import pandas as pd
import os
import json



def compare_json(json1, json2):
    """
    Compare two JSON objects and return a list of differences.
    Returns a list of dicts with 'path', 'value1', 'value2' for differences.
    """
    def deep_diff(path, val1, val2):
        diffs = []
        if type(val1) != type(val2):
            diffs.append({"path": path, "value1": val1, "value2": val2})
        elif isinstance(val1, dict):
            keys = set(val1.keys()) | set(val2.keys())
            for key in keys:
                new_path = f"{path}.{key}" if path else key
                if key not in val1:
                    diffs.append({"path": new_path, "value1": None, "value2": val2[key]})
                elif key not in val2:
                    diffs.append({"path": new_path, "value1": val1[key], "value2": None})
                else:
                    diffs.extend(deep_diff(new_path, val1[key], val2[key]))
        elif isinstance(val1, list):
            if len(val1) != len(val2):
                diffs.append({"path": path, "value1": val1, "value2": val2})
            else:
                for i, (v1, v2) in enumerate(zip(val1, val2)):
                    new_path = f"{path}[{i}]"
                    diffs.extend(deep_diff(new_path, v1, v2))
        elif val1 != val2:
            diffs.append({"path": path, "value1": val1, "value2": val2})
        return diffs

    return deep_diff("", json1, json2)

def get_json_length_metrics(data):
    metrics = {}

    metrics["top_level_keys"] = len(data.keys())

    if "data" in data:
        metrics["data_keys"] = len(data["data"].keys())

        # Example for GBFS feeds
        if "en" in data["data"]:
            metrics["feeds_count"] = len(data["data"]["en"].get("feeds", []))

    return metrics

if __name__ == "__main__":
    compare_json,
    get_json_length_metrics