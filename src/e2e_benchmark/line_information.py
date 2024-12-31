import json


def count_lines(txt_file):
    with open(txt_file, 'r') as f:
        return len(f.readlines())


def process_jsonl(jsonl_file, txt_dir):
    updated_lines = []
    with open(jsonl_file, 'r') as f:
        lines = f.readlines()

    for line in lines:
        json_obj = json.loads(line.strip())

        # Check if "accept" key exists and its value is [2]
        if json_obj.get("accept") == [2]:
            json_id = json_obj.get('id', '').split('_')[0]
            txt_file_path = f'{txt_dir}/{json_id}.txt'

            try:
                num_lines = count_lines(txt_file_path)
                json_obj['line'] = num_lines
            except FileNotFoundError:
                pass

            updated_lines.append(json_obj)

    return updated_lines


def write_updated_jsonl(updated_lines, output_file):
    with open(output_file, 'w', encoding='utf-8') as f:
        for updated_line in updated_lines:
            f.write(json.dumps(updated_line, ensure_ascii=False) + '\n')


def main(jsonl_dir, txt_dir, jsonl_filename):
    jsonl_file = f'{jsonl_dir}/{jsonl_filename}'
    updated_lines = process_jsonl(jsonl_file, txt_dir)
    output_file = f'{jsonl_dir}/updated_{jsonl_filename}'
    write_updated_jsonl(updated_lines, output_file)


if __name__ == '__main__':
    jsonl_dir = 'data/pering_line_to_text'
    txt_dir = 'data/text'
    jsonl_filename = 'pering.jsonl'
    main(jsonl_dir, txt_dir, jsonl_filename)
