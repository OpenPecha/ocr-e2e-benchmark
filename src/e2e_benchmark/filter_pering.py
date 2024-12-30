import os
import json
import re


def extract_ids_from_file(file_path):

    ids = set()
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            data = json.loads(line)
            id_without_extension = os.path.splitext(data["id"])[0]
            ids.add(id_without_extension)
    return ids


def clean_image_url(image_url):
    return re.sub(r'(\?.*)$', '', image_url)


def process_directory_and_match_ids(directory_path, match_ids):
    matched_records = []
    for filename in os.listdir(directory_path):
        if filename.endswith(".jsonl"):
            file_path = os.path.join(directory_path, filename)
            with open(file_path, "r", encoding="utf-8") as file:
                for line_number, line in enumerate(file, start=1):
                    try:
                        data = json.loads(line.strip())
                        dir_file_id = data["id"].split("_")[0]
                        dir_file_id = os.path.splitext(dir_file_id)[0]
                        if dir_file_id in match_ids:

                            if "image" in data:
                                data["image"] = clean_image_url(data["image"])
                            matched_records.append(data)
                    except json.JSONDecodeError as e:
                        print(f"Skipping malformed JSON in {filename} at line {line_number}: {e}")
    return matched_records


def save_to_jsonl(file_path, data):
    with open(file_path, "w", encoding="utf-8") as file:
        for record in data:
            json.dump(record, file, ensure_ascii=False)
            file.write("\n")


def main(pering_la_path, jsonl_dir_path, output_path):
    match_ids = extract_ids_from_file(pering_la_path)
    matched_records = process_directory_and_match_ids(jsonl_dir_path, match_ids)
    save_to_jsonl(output_path, matched_records)
    print("Process completed successfully.")


if __name__ == "__main__":
    pering_la_path = "data/pering_la.jsonl"
    jsonl_dir_path = "data/deduplicate_b11-b18"
    output_path = "data/pering_line_to_text/pering.jsonl"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    main(pering_la_path, jsonl_dir_path, output_path)
