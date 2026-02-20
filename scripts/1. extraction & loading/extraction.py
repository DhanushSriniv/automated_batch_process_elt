import os
from el_global import (
    get_package_metadata,
    get_resource_metadata,
    fetch_json,
    extract_feeds,
    save_feed_to_csv
)

# Load environment variables from . import load_env
BASE_URL = os.getenv("BASE_URL")
DATASET_ID = os.getenv("DATASET_ID")
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER")


def run_pipeline():
    package = get_package_metadata(BASE_URL, DATASET_ID)

    for resource in package["result"]["resources"]:

        if not resource["datastore_active"]:

            metadata = get_resource_metadata(BASE_URL, resource["id"])

            name = metadata["result"]["name"]
            file_url = metadata["result"]["url"]

            if name in [
                "bike-share-json",
                "bike-share-gbfs-general-bikeshare-feed-specification"
            ]:

                print(f"\nProcessing Resource: {name}")
                print("=" * 60)

                root_json = fetch_json(file_url)

                feeds = extract_feeds(root_json)

                for feed in feeds:
                    feed_name = feed["name"]
                    feed_url = feed["url"]

                    print(f"\nFeed: {feed_name}")
                    print(f"URL: {feed_url}")

                    feed_data = fetch_json(feed_url)

                    # Print sample JSON
                    print("Sample JSON:")
                    print({
                        "last_updated": feed_data.get("last_updated"),
                        "ttl": feed_data.get("ttl"),
                        "data_keys": list(feed_data.get("data", {}).keys())
                    })

                    # Save to CSV
                    file_path = save_feed_to_csv(
                        feed_name.replace(" ", "_"),
                        feed_data,
                        OUTPUT_FOLDER
                    )

                    print(f"Saved to: {file_path}")
                    print("-" * 50)
