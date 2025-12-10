import os
import json
import re
import argparse
import cv2
import numpy as np

# --- SageMaker Input/Output Directories (FIXED) ---
ANNOTATIONS_INPUT_DIR = '/opt/ml/processing/input/annotations'
IMAGES_INPUT_DIR = '/opt/ml/processing/input/images'
CROPS_OUTPUT_DIR = '/opt/ml/processing/output/crops'

# NEW PATH for the output JSON file
ANNOTATIONS_OUTPUT_FILE = '/opt/ml/processing/output/annotations/final_annotations.json'

# --- Global Storage for Transformed Annotations ---
transformed_annotations = []

# --- Utility Functions ---

def clean_s3_key(file_upload_value):
    """Cleans the 'file_upload' path to get the exact image filename, removing the leading hash."""
    if not file_upload_value: return None
    filename_with_hash = os.path.basename(file_upload_value)
    match = re.search(r'-', filename_with_hash)
    return filename_with_hash[match.end():] if match else filename_with_hash

def normalize_to_pixels(normalized_x, normalized_y, original_width, original_height):
    """Converts normalized (0-100%) coordinates to pixel coordinates."""
    pixel_x = int(normalized_x * original_width / 100.0)
    pixel_y = int(normalized_y * original_height / 100.0)
    return (pixel_x, pixel_y)

def crop_and_transform_annotations(image_path, pole_info, crop_size, output_dir, original_filename):
    """Crops the image, transforms the pole annotation coordinates, and stores the new annotation."""
    global transformed_annotations

    img = cv2.imread(image_path)
    if img is None:
        print(f"ERROR: Could not read image at {image_path}")
        return 0
        
    H, W, _ = img.shape
    half_crop = crop_size // 2
    
    orig_x_center, orig_y_center = pole_info['pixel_center']
    
    # Calculate the crop window (clamping to image boundaries)
    x_min_crop = max(0, orig_x_center - half_crop)
    y_min_crop = max(0, orig_y_center - half_crop)
    x_max_crop = min(W, orig_x_center + half_crop)
    y_max_crop = min(H, orig_y_center + half_crop)
    
    # Boundary clamping adjustment
    if x_max_crop - x_min_crop < crop_size:
        if x_min_crop == 0: x_max_crop = min(W, x_min_crop + crop_size)
        elif x_max_crop == W: x_min_crop = max(0, x_max_crop - crop_size)
            
    if y_max_crop - y_min_crop < crop_size:
        if y_min_crop == 0: y_max_crop = min(H, y_min_crop + crop_size)
        elif y_max_crop == H: y_min_crop = max(0, y_max_crop - crop_size)

    crop_img = img[y_min_crop:y_max_crop, x_min_crop:x_max_crop]
    crop_height, crop_width, _ = crop_img.shape
    
    # --- 1. Coordinate Transformation ---
    
    # New pixel coordinates relative to the top-left of the crop (x_min_crop, y_min_crop)
    new_x_min_pixel = max(0, pole_info['pixel_x_min'] - x_min_crop)
    new_y_min_pixel = max(0, pole_info['pixel_y_min'] - y_min_crop)
    new_x_max_pixel = min(crop_width, pole_info['pixel_x_max'] - x_min_crop)
    new_y_max_pixel = min(crop_height, pole_info['pixel_y_max'] - y_min_crop)
    
    new_width_pixel = new_x_max_pixel - new_x_min_pixel
    new_height_pixel = new_y_max_pixel - new_y_min_pixel

    # Calculate new normalized coordinates (0-100%) for the cropped image
    new_normalized_x = (new_x_min_pixel / crop_width) * 100.0
    new_normalized_y = (new_y_min_pixel / crop_height) * 100.0
    new_normalized_width = (new_width_pixel / crop_width) * 100.0
    new_normalized_height = (new_height_pixel / crop_height) * 100.0
    
    # --- 2. Save Image and Create Annotation Entry ---
    
    base_name, ext = os.path.splitext(original_filename)
    output_filename = f"{base_name}_pole_{pole_info['id']}_{crop_size}{ext}"
    output_path = os.path.join(output_dir, output_filename)
    
    cv2.imwrite(output_path, crop_img)
    
    # Construct the new Label Studio style annotation structure for the cropped image
    new_annotation_entry = {
        "file_upload": f"/data/upload/{output_filename}", 
        "data": {"image": f"/data/upload/{output_filename}"},
        "annotations": [{
            "result": [{
                "id": pole_info['id'], 
                "type": "rectanglelabels",
                "original_width": crop_width,
                "original_height": crop_height,
                "value": {
                    "x": round(new_normalized_x, 4),
                    "y": round(new_normalized_y, 4),
                    "width": round(new_normalized_width, 4),
                    "height": round(new_normalized_height, 4),
                    "rectanglelabels": ["Pole"]
                }
            }]
        }]
    }
    
    transformed_annotations.append(new_annotation_entry)
    return 1

# --- Main Processing Logic ---

def process_single_annotation_file(local_annotation_path, crop_size_pixels):
    """Main function logic to iterate over annotations, transform, and crop."""
    
    with open(local_annotation_path, 'r') as f:
        tasks_data = json.load(f) 
    
    crops_created_count = 0

    for task in tasks_data:
        s3_path_full = task.get('file_upload')
        s3_key = clean_s3_key(s3_path_full)
        if not s3_key: continue
            
        local_image_path = os.path.join(IMAGES_INPUT_DIR, s3_key)
        if not os.path.exists(local_image_path): continue
        
        annotations = task.get('annotations', [])
        if not annotations or not annotations[0].get('result'): continue

        for i, result in enumerate(annotations[0]['result']):
            
            original_width = result.get('original_width')
            original_height = result.get('original_height')

            value = result.get('value', {})
            labels = value.get('rectanglelabels', [])
            
            if original_width and 'Pole' in labels and 'x' in value:
                
                # Extract normalized bounding box coordinates
                norm_x = value['x']
                norm_y = value['y']
                norm_width = value['width']
                norm_height = value['height']
                
                # Convert corners to pixel coordinates
                pixel_x_min, pixel_y_min = normalize_to_pixels(norm_x, norm_y, original_width, original_height)
                pixel_x_max, pixel_y_max = normalize_to_pixels(norm_x + norm_width, norm_y + norm_height, original_width, original_height)
                
                # Calculate center point in pixels (for defining the crop center)
                center_x_normalized = norm_x + (norm_width / 2)
                center_y_normalized = norm_y + (norm_height / 2)
                pixel_center_x, pixel_center_y = normalize_to_pixels(center_x_normalized, center_y_normalized, original_width, original_height)
                
                pole_info = {
                    'id': result.get('id', result.get('unique_id', f'pole_{i}')),
                    'pixel_center': (pixel_center_x, pixel_center_y),
                    'pixel_x_min': pixel_x_min,
                    'pixel_y_min': pixel_y_min,
                    'pixel_x_max': pixel_x_max,
                    'pixel_y_max': pixel_y_max
                }

                # Perform Cropping and Annotation Transformation
                new_crops = crop_and_transform_annotations(
                    image_path=local_image_path,
                    pole_info=pole_info,
                    crop_size=crop_size_pixels,
                    output_dir=CROPS_OUTPUT_DIR,
                    original_filename=s3_key
                )
                crops_created_count += new_crops
            
    return crops_created_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--crop-size', type=int, default=256, help='The size of the square crop in pixels.')
    args, _ = parser.parse_known_args()
    
    # Create all necessary output directories
    os.makedirs(CROPS_OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(ANNOTATIONS_OUTPUT_FILE), exist_ok=True)
    
    total_crops_created = 0
    
    # Process all annotation files
    if os.path.exists(ANNOTATIONS_INPUT_DIR):
        for annotation_file in os.listdir(ANNOTATIONS_INPUT_DIR):
            if annotation_file.endswith('.json'):
                local_annotation_path = os.path.join(ANNOTATIONS_INPUT_DIR, annotation_file)
                crops_count = process_single_annotation_file(local_annotation_path, args.crop_size)
                total_crops_created += crops_count
            
    # --- Final step: Write the combined annotation JSON file ---
    with open(ANNOTATIONS_OUTPUT_FILE, 'w') as f:
        json.dump(transformed_annotations, f, indent=4)
        
    print(f"Processing script finished. Total crops created: {total_crops_created}")
    print(f"Final annotation file created at: {ANNOTATIONS_OUTPUT_FILE}")
