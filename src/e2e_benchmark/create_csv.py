import os
import json
import csv
from collections import defaultdict
from multiprocessing import Pool, cpu_count

URL_PREFIX = "https://s3.amazonaws.com/monlam.ai.ocr/e2e_benchmark/"
GROUP_ID = 1
BATCH_ID = 1
STATE = "post_correction"


def read_jsonl(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line.strip()))
    return data


def group_by_split_id(data):
    grouped = defaultdict(list)
    for item in data:
        split_id = item['id'].split('_')[0]
        grouped[split_id].append(item)
    return grouped


def process_matching_lines(group, split_id):
    """Process groups where the line count matches the line values."""
    ids_with_coordinates = []

    for item in group:
        if 'line' not in item:
            print(f"Warning: Missing 'line' key for ID {item.get('id', 'unknown')}")
            continue

        coordinates = '_'.join(item['id'].split('_')[1:])
        y1 = int(coordinates.split('-')[1])
        ids_with_coordinates.append((item['id'], coordinates, y1, item.get('user_input', '')))

    if not ids_with_coordinates:
        return []

    # Sort by y1
    ids_with_coordinates.sort(key=lambda x: x[2])

    # Rename IDs with line numbers
    updated_ids = []
    for idx, (full_id, coordinates, _, text) in enumerate(ids_with_coordinates, start=1):
        updated_id = f"{split_id}_{idx}.png"
        updated_ids.append((updated_id, text))

    return updated_ids


def process_mismatched_lines(split_id, text_dir):
    """Process groups where the line count does not match the line values."""
    text_file_path = os.path.join(text_dir, f"{split_id}.txt")
    if not os.path.exists(text_file_path):
        print(f"Warning: Text file for {split_id} not found.")
        return []

    # Read the text file and split into lines
    with open(text_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Generate IDs for each line
    mismatched_ids = []
    for idx, line in enumerate(lines, start=1):
        image_id = f"{split_id}_{idx}.png"
        mismatched_ids.append((image_id, line.strip()))

    return mismatched_ids


def match_text_with_images(processed_data, image_dir):
    """Match text with image files in the directory."""
    images = {img for img in os.listdir(image_dir) if img.endswith('.png')}
    matched_data = []

    for item in processed_data:
        image_id, text = item
        if image_id in images:
            matched_data.append((image_id, text))

    return matched_data


def process_jsonl_file(args):
    jsonl_file, jsonl_dir, text_dir, image_dir = args
    all_processed_data = []

    jsonl_path = os.path.join(jsonl_dir, jsonl_file)
    jsonl_data = read_jsonl(jsonl_path)

    for item in jsonl_data:
        if 'id' not in item or 'line' not in item:
            print(f"Warning: Missing required keys in {jsonl_file}: {item}")
            continue

    # Group by split ID
    grouped_data = group_by_split_id(jsonl_data)

    for split_id, group in grouped_data.items():
        if all('line' in item for item in group) and len(group) == group[0]['line']:
            processed = process_matching_lines(group, split_id)
        else:
            processed = process_mismatched_lines(split_id, text_dir)
        matched = match_text_with_images(processed, image_dir)

        for image_id, text in matched:
            all_processed_data.append([
                image_id, GROUP_ID, BATCH_ID, STATE, text, f"{URL_PREFIX}{image_id}"
            ])

    return all_processed_data


def write_csv(output_file, data):
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["id", "group_id", "batch_id", "state", "text", "url"])
        for row in data:
            writer.writerow(row)


def main(jsonl_dir, text_dir, image_dir, output_csv):
    jsonl_files = [f for f in os.listdir(jsonl_dir) if f.endswith('.jsonl')]
    args = [(jsonl_file, jsonl_dir, text_dir, image_dir) for jsonl_file in jsonl_files]

    with Pool(cpu_count()) as pool:
        results = pool.map(process_jsonl_file, args)
    all_processed_data = [item for sublist in results for item in sublist]
    write_csv(output_csv, all_processed_data)


if __name__ == "__main__":
    jsonl_dir = "data/pering_line_to_text"
    text_dir = "data/text"
    image_dir = "data/line_images"
    output_csv = "data/csv_output/e2e_output.csv"

    main(jsonl_dir, text_dir, image_dir, output_csv)
