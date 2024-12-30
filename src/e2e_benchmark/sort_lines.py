import json
import os
from collections import defaultdict


def save_to_jsonl(file_path, data):
    with open(file_path, "w", encoding="utf-8") as file:
        for record in data:
            json.dump(record, file, ensure_ascii=False)
            file.write("\n")


def process_jsonl_files(dir_path):
    """
    Process all JSONL files in the specified directory.
    Groups by line_image_name, orders by y1, and renames 'id' to line_image_n.
    """
    all_data = defaultdict(list)
    for filename in os.listdir(dir_path):
        if filename.endswith(".jsonl"):
            file_path = os.path.join(dir_path, filename)
            with open(file_path, "r", encoding="utf-8") as file:
                for line in file:
                    record = json.loads(line)
                    line_image_name = record["id"].split("_")[0]
                    line_image_coordinates = record["id"].split("_")[1]
                    y1 = int(line_image_coordinates.split("-")[1].split("_")[0])
                    all_data[line_image_name].append((y1, record))

    result_data = []
    for line_image_name, records in all_data.items():

        records.sort(key=lambda x: x[0])
        for idx, (y1, record) in enumerate(records, start=1):
            record["id"] = f"{line_image_name}_{idx}"
            result_data.append(record)

    return result_data


def main():
    dir_path = "data/test"
    output_path = "data/sorted_lines/sorted.jsonl"
    processed_data = process_jsonl_files(dir_path)
    save_to_jsonl(output_path, processed_data)


if __name__ == "__main__":
    main()
