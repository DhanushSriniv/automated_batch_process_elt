import os
import sys

# Dynamically find the project root by looking for the 'utils' folder
current_dir = os.path.dirname(os.path.abspath(__file__))
while not os.path.exists(os.path.join(current_dir, 'utils')):
    parent = os.path.dirname(current_dir)
    if parent == current_dir:
        raise RuntimeError("Could not find project root (utils folder not found)")
    current_dir = parent
sys.path.insert(0, current_dir)

from utils.el_global import (
    load_json_from_file
)

from utils.data_validataion import compare_json, get_json_length_metrics

json1 = load_json_from_file(os.path.join(current_dir, "data/gbfs_feeds/feeds_data/bike-share-json/feeds_summary.json"))
json2 = load_json_from_file(os.path.join(current_dir, "data/gbfs_feeds/feeds_data/gbfs-specification/feeds_summary.json"))

diffs = compare_json(json1, json2)

if diffs:
    print("\nDifferences Found:")
    for d in diffs:
        print(d)
else:
    print("\nJSON structures are identical.")
    


live_metrics = get_json_length_metrics(json1)
spec_metrics = get_json_length_metrics(json2)

if live_metrics != spec_metrics:
    print("Length mismatch detected")
else:
    print("Length validation passed")
    
