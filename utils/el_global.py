import requests
import pandas as pd
import os
import json


def get_package_metadata(base_url, dataset_id):
    url = f"{base_url}/api/3/action/package_show"
    params = {"id": dataset_id}
    return requests.get(url, params=params).json()


def get_resource_metadata(base_url, resource_id):
    url = f"{base_url}/api/3/action/resource_show?id={resource_id}"
    return requests.get(url).json()


def fetch_json(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def load_json_from_file(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)
    

def convert_json_to_csv(json_data, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    file_path = os.path.join(output_folder, "output.csv")
    df = pd.json_normalize(json_data)
    df.to_csv(file_path, index=False)
    return file_path


def extract_feeds(root_json):
    return root_json.get("data", {}).get("en", {}).get("feeds", [])


def save_feed_to_csv(feed_name, feed_data, output_folder):
    data_section = feed_data.get("data", {})

    # Detect correct nested list
    if "stations" in data_section:
        records = data_section["stations"]
    elif "plans" in data_section:
        records = data_section["plans"]
    elif "regions" in data_section:
        records = data_section["regions"]
    else:
        records = [data_section]

    df = pd.json_normalize(records)

    os.makedirs(output_folder, exist_ok=True)
    file_path = os.path.join(output_folder, f"{feed_name}.csv")
    df.to_csv(file_path, index=False)

    return file_path


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


# Load Json data from file and store as csv file in the output folder
# Folder: data/output_data/

if __name__ == "__main__":
    get_package_metadata,
    get_resource_metadata,
    fetch_json,
    load_json_from_file,
    extract_feeds,
    save_feed_to_csv