import requests
import pandas as pd
import os


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

if __name__ == "__main__":
    get_package_metadata,
    get_resource_metadata,
    fetch_json,
    extract_feeds,
    save_feed_to_csv