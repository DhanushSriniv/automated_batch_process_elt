import json
import os
from pathlib import Path

def get_json_schema(data):
    """Extract basic schema from JSON data"""
    if isinstance(data, dict):
        return {key: get_json_schema(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [get_json_schema(data[0])] if data else []
    else:
        return type(data).__name__

def print_json_schemas(folder_paths):
    """Print schemas for all JSON files in given folders"""
    for folder_path in folder_paths:
        print(f"\n{'='*60}")
        print(f"Folder: {folder_path}")
        print(f"{'='*60}\n")
        
        json_files = [f for f in os.listdir(folder_path) 
                     if f.endswith('.json') and f != 'feeds_summary.json']
        
        for json_file in sorted(json_files):
            file_path = os.path.join(folder_path, json_file)
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    schema = get_json_schema(data['data'])
                    print(f"File: {json_file}")
                    print(f"Schema:\n{json.dumps(schema, indent=2)}\n")
            except Exception as e:
                print(f"Error reading {json_file}: {e}\n")

# Define folder paths
folders = [
    r"C:\Users\Administrator\OneDrive\Desktop\Portfolio Building\Data Engineer\Projects\2. Automated Batch Processing Data\2. Github Repo\automated_batch_process_elt\data\gbfs_feeds\feeds_data\bike-share-json",
    r"C:\Users\Administrator\OneDrive\Desktop\Portfolio Building\Data Engineer\Projects\2. Automated Batch Processing Data\2. Github Repo\automated_batch_process_elt\data\gbfs_feeds\feeds_data\gbfs-specification"
]

print_json_schemas(folders)
