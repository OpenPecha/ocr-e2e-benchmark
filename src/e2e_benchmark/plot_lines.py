import json
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import random


def extract_and_plot(jsonl_file, image_file):
    img = mpimg.imread(image_file)
    img_height, img_width = img.shape[:2]

    plt.figure(figsize=(12, 12))
    plt.imshow(img)
    plt.title('Bounding Boxes Overlaid on Image')
    plt.axis('on')

    colors = []
    labels = []
    used_colors = set()

    with open(jsonl_file, 'r') as file:
        for line in file:
            data = json.loads(line)
            if 'id' in data:
                id_value = data['id']
                labels.append(id_value)
                coordinates_str = '_'.join(id_value.split('_')[1:])
                coordinates = [tuple(map(int, coord.split('-'))) for coord in coordinates_str.split('_')]
                x_coords = [coord[0] for coord in coordinates]  # X stays the same
                y_coords = [coord[1] for coord in coordinates]  # Y stays the same for top-left origin
                x_coords.append(x_coords[0])  # Close the polygon
                y_coords.append(y_coords[0])  # Close the polygon
                while True:
                    color = "#{:06x}".format(random.randint(0, 0xFFFFFF))
                    if color not in used_colors:
                        used_colors.add(color)
                        break
                colors.append(color)
                plt.plot(x_coords, y_coords, marker='o', color=color, label=id_value)

    plt.grid(False)
    plt.tight_layout()

    plt.figure(figsize=(4, len(labels) * 0.5))
    for color, label in zip(colors, labels):
        plt.plot([], [], color=color, label=label)
    plt.title('Bounding Box Legend')
    plt.axis('off')
    plt.legend(loc='upper left', title="Bounding Boxes", fontsize='small')
    plt.tight_layout()

    plt.show()


jsonl_file = 'data/test/test.jsonl'
image_file = '/Users/tenkal/OpenPecha/ocr-e2e-benchmark/data/source_images/layout_analysis_images/51980774.jpg'
extract_and_plot(jsonl_file, image_file)
