import os
import json
import re
import argparse
import cv2  # OpenCV for image processing
import numpy as np

# --- SageMaker Input/Output Directories ---
ANNOTATIONS_INPUT_DIR = '/opt/ml/processing/input/annotations'
IMAGES_INPUT_DIR = '/opt/ml/processing/input/images'
CROPS_OUTPUT_DIR = '/opt/ml/processing/output/crops'

# --- Utility Functions ---

def clean_s3_key(file_upload_value):
    """
    Cleans the 'file_upload' path to get the exact image filename, removing the leading hash.
    e.g., /data/upload/HASH-FILENAME.jpg -> FILENAME.jpg
    """
    if not file_upload_value:
        return None
    
    filename_with_hash = os.path.basename(file_upload_value)
    
    # Remove the hash and dash (e.g., "d1f998ba-")
    match = re.search(r'-', filename_with_hash)
    return filename_with_hash[match.end():] if match else filename_with_hash

def normalize_to_pixels(normalized_x, normalized_y, original_width, original_height):
    """Converts normalized (0-100%) coordinates to pixel coordinates."""
    pixel_x = int(normalized_x * original_width / 100.0)
    pixel_y = int(normalized_y * original_height / 100.0)
    return (pixel_x, pixel_y)

def crop_around_poles(image_path, pole_locations, crop_size, output_dir, original_filename):
    """
    Crops the image around each pole location and saves the result.
    
    Args:
        image_path (str): Local path to the source image.
        pole_locations (list): List of (x, y) pixel coordinates for pole centers.
        crop_size (int): The width/height of the square crop in pixels.
        output_dir (str): Local directory to save the cropped images.
        original_filename (str): Base name of the original file for naming crops.
        
    Returns:
        int: Number of crops successfully created.
    """
    img = cv2.imread(image_path)
    if img is None:
        print(f"ERROR: Could not read image at {image_path}")
        return 0
        
    H, W, _ = img.shape
    half_crop = crop_size // 2
    crops_created = 0
    base_name, ext = os.path.splitext(original_filename)

    for i, (center_x, center_y) in enumerate(pole_locations):
        # Calculate the bounding box coordinates (clamping to image boundaries)
        x_min = max(0, center_x - half_crop)
        y_min = max(0, center_y - half_crop)
        x_max = min(W, center_x + half_crop)
        y_max = min(H, center_y + half_crop)

        # Handle boundary conditions by shifting the crop window
        if x_max - x_min < crop_size:
            if x_min == 0:
                x_max = min(W, x_min + crop_size)
            elif x_max == W:
                x_min = max(0, x_max - crop_size)
                
        if y_max - y_min < crop_size:
            if y_min == 0:
                y_max = min(H, y_min + crop_size)
            elif y_max == H:
                y_min = max(0, y_max - crop_size)

        # Perform the crop
        crop_img = img[y_min:y_max, x_min:x_max]
        
        # Ensure the cropped image is exactly the requested size (if needed, pad it)
        # For simplicity, we save the potentially boundary-adjusted crop.
        
        # Construct the output filename
        output_filename = f"{base_name}_crop_{i:03d}{ext}"
        output_path = os.path.join(output_dir, output_filename)
        
        # Save the crop to the Sagemaker output path
        cv2.imwrite(output_path, crop_img)
        crops_created += 1

    return crops_created

# --- Main Processing Logic ---

def process_single_annotation_file(local_annotation_path, crop_size_pixels):
    """Processes a single local JSON file."""
    
    with open(local_annotation_path, 'r') as f:
        tasks_data = json.load(f) 
    
    crops_created_count = 0

    print(f"Processing {len(tasks_data)} tasks from {os.path.basename(local_annotation_path)}.")

    for task in tasks_data:
        # 1. Clean S3 Key to find the local image file
        s3_path_full = task.get('file_upload')
        s3_key = clean_s3_key(s3_path_full)
        if not s3_key: continue
            
        local_image_path = os.path.join(IMAGES_INPUT_DIR, s3_key)

        if not os.path.exists(local_image_path):
            print(f"Warning: Image file not found at {local_image_path}. Skipping task.")
            continue
        
        # 2. Extract and Convert Pole Locations
        pole_locations_pixels = []
        original_width, original_height = None, None
        
        annotations = task.get('annotations', [])
        if not annotations or not annotations[0].get('result'): continue

        # The core logic for parsing the nested Label Studio JSON
        for result in annotations[0]['result']:
            if original_width is None:
                original_width = result.get('original_width')
                original_height = result.get('original_height')

            value = result.get('value', {})
            labels = value.get('rectanglelabels', [])
            
            if original_width and 'Pole' in labels and 'x' in value:
                # Calculate center point from bounding box (x, y, width, height are normalized)
                center_x_normalized = value['x'] + (value['width'] / 2)
                center_y_normalized = value['y'] + (value['height'] / 2)
                
                # Convert normalized center coordinates to pixels
                pixel_center_x, pixel_center_y = normalize_to_pixels(
                    center_x_normalized, center_y_normalized, original_width, original_height
                )
                pole_locations_pixels.append((pixel_center_x, pixel_center_y))

        if not pole_locations_pixels: continue

        # 3. Perform Cropping
        new_crops = crop_around_poles(
            image_path=local_image_path,
            pole_locations=pole_locations_pixels,
            crop_size=crop_size_pixels,
            output_dir=CROPS_OUTPUT_DIR,
            original_filename=s3_key # The cleaned filename
        )
        crops_created_count += new_crops
            
    return crops_created_count


if __name__ == '__main__':
    # Define and parse arguments passed by SageMaker
    parser = argparse.ArgumentParser()
    parser.add_argument('--crop-size', type=int, default=256, help='The size of the square crop in pixels.')
    args, _ = parser.parse_known_args()
    
    os.makedirs(CROPS_OUTPUT_DIR, exist_ok=True)
    
    total_crops_created = 0
    
    # Iterate over all JSON files downloaded by SageMaker
    if os.path.exists(ANNOTATIONS_INPUT_DIR):
        for annotation_file in os.listdir(ANNOTATIONS_INPUT_DIR):
            if annotation_file.endswith('.json'):
                local_annotation_path = os.path.join(ANNOTATIONS_INPUT_DIR, annotation_file)
                crops_count = process_single_annotation_file(local_annotation_path, args.crop_size)
                total_crops_created += crops_count
            
    print(f"Processing script finished. Total crops created and saved to {CROPS_OUTPUT_DIR}: {total_crops_created}")
