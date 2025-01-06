import os
import tempfile
import pytest
import json
from src.e2e_benchmark.create_csv import read_jsonl, group_by_split_id, process_matching_lines, process_mismatched_lines, match_text_with_images, write_csv


def test_read_jsonl():
    data = [{"id": "test_1", "line": 1}, {"id": "test_2", "line": 2}]
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.jsonl') as temp_file:
        for item in data:
            temp_file.write(json.dumps(item) + '\n')
        temp_path = temp_file.name

    read_data = read_jsonl(temp_path)
    assert read_data == data
    os.remove(temp_path)


def test_group_by_split_id():
    data = [
        {"id": "split1_1", "line": 1},
        {"id": "split1_2", "line": 2},
        {"id": "split2_1", "line": 1}
    ]
    grouped = group_by_split_id(data)
    assert len(grouped) == 2
    assert "split1" in grouped
    assert len(grouped["split1"]) == 2


def test_process_matching_lines():
    group = [
        {"id": "split1_1-100", "line": 1, "user_input": "Text1"},
        {"id": "split1_2-200", "line": 1, "user_input": "Text2"}
    ]
    processed = process_matching_lines(group, "split1")
    assert len(processed) == 2
    assert processed[0][0] == "split1_1.png"
    assert processed[0][1] == "Text1"


def test_process_mismatched_lines():
    split_id = "split1"
    lines = ["Line 1", "Line 2"]
    with tempfile.TemporaryDirectory() as temp_dir:
        text_file_path = os.path.join(temp_dir, f"{split_id}.txt")
        with open(text_file_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))

        processed = process_mismatched_lines(split_id, temp_dir)
        assert len(processed) == 2
        assert processed[0][0] == f"{split_id}_1.png"
        assert processed[0][1] == "Line 1"


def test_match_text_with_images():
    processed_data = [("image1.png", "Text1"), ("image2.png", "Text2")]
    with tempfile.TemporaryDirectory() as temp_dir:
        open(os.path.join(temp_dir, "image1.png"), 'w').close()

        matched = match_text_with_images(processed_data, temp_dir)
        assert len(matched) == 1
        assert matched[0][0] == "image1.png"
        assert matched[0][1] == "Text1"


def test_write_csv():
    data = [
        ["id1", 1, 1, "state1", "Text1", "url1"],
        ["id2", 1, 1, "state2", "Text2", "url2"]
    ]
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.csv') as temp_file:
        temp_path = temp_file.name

    write_csv(temp_path, data)

    with open(temp_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    assert lines[0].strip() == "id,group_id,batch_id,state,text,url"
    assert lines[1].strip() == "id1,1,1,state1,Text1,url1"
    os.remove(temp_path)
