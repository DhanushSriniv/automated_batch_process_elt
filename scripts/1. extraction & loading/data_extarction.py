# data_extraction.py
import os
import sys
import uuid

current_dir = os.path.dirname(os.path.abspath(__file__))
while not os.path.exists(os.path.join(current_dir, 'utils')):
    parent = os.path.dirname(current_dir)
    if parent == current_dir:
        raise RuntimeError("Could not find project root (utils folder not found)")
    current_dir = parent
sys.path.insert(0, current_dir)

from utils.el_global import (
    get_package_metadata,
    get_resource_metadata,
    fetch_json,
    extract_feeds,
    save_feed_to_csv,
)
from utils.bronze_loader import load_feed_to_bronze  # NEW

# load_env() if you have it
BASE_URL = os.getenv("BASE_URL")
DATASET_ID = os.getenv("DATASET_ID")
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER", "data/feeds_data")

def run_pipeline():
    batch_id = str(uuid.uuid4())

    package = get_package_metadata(BASE_URL, DATASET_ID)
    for resource in package["result"]["resources"]:
        if not resource["datastore_active"]:
            metadata = get_resource_metadata(BASE_URL, resource["id"])
            name = metadata["result"]["name"]
            file_url = metadata["result"]["url"]

            if name in [
                "bike-share-json",
                "bike-share-gbfs-general-bikeshare-feed-specification",
            ]:
                print(f"\nProcessing Resource: {name}")
                print("=" * 60)

                root_json = fetch_json(file_url)
                feeds = extract_feeds(root_json)

                for feed in feeds:
                    feed_name = feed["name"]              # e.g. 'station_information'[file:37]
                    feed_url = feed["url"]

                    print(f"\nFeed: {feed_name}")
                    print(f"URL: {feed_url}")

                    feed_data = fetch_json(feed_url)

                    # OPTIONAL: sample print
                    print("Sample JSON:")
                    print({
                        "last_updated": feed_data.get("last_updated"),
                        "ttl": feed_data.get("ttl"),
                        "data_keys": list(feed_data.get("data", {}).keys())
                    })

                    # 1) Save to CSV (for exploration, can be removed later)
                    file_path = save_feed_to_csv(
                        feed_name.replace(" ", "_"),
                        feed_data,
                        OUTPUT_FOLDER,
                    )
                    print(f"Saved CSV to: {file_path}")

                    # 2) Load RAW JSON to Bronze
                    load_feed_to_bronze(
                        feed_name=feed_name,
                        source_name=name,      # which resource this came from
                        batch_id=batch_id,
                        api_url=feed_url,
                        payload=feed_data,
                    )

                    print("Loaded to bronze.gbfs_feed_raw")
                    print("-" * 50)

if __name__ == "__main__":
    run_pipeline()
