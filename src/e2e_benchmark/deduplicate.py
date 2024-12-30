import json
import os


def process_jsonl_file(input_file_path, output_file_path):
    seen_ids = set()
    unique_entries = []

    with open(input_file_path, "r", encoding="utf-8") as infile:
        for line in infile:
            try:
                entry = json.loads(line)
                entry_id = entry.get("id")
                if entry_id and entry_id not in seen_ids:
                    seen_ids.add(entry_id)
                    unique_entries.append(entry)
            except json.JSONDecodeError:
                print(f"Error decoding JSON in file {input_file_path}: {line}")

    with open(output_file_path, "w", encoding="utf-8") as outfile:
        for entry in unique_entries:
            outfile.write(json.dumps(entry, ensure_ascii=False) + "\n")

    print(f"Processed {input_file_path}: {len(unique_entries)} unique entries saved to {output_file_path}.")


def process_directory(input_directory, output_directory):
    os.makedirs(output_directory, exist_ok=True)

    for filename in os.listdir(input_directory):
        if filename.endswith(".jsonl"):
            input_file_path = os.path.join(input_directory, filename)
            output_file_path = os.path.join(output_directory, f"deduplicate_{filename}")
            process_jsonl_file(input_file_path, output_file_path)


def main():
    input_directory = "data/b11_to_b18"
    output_directory = "data/deduplicate_b11-b18"

    process_directory(input_directory, output_directory)


if __name__ == "__main__":
    main()
