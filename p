import boto3
import xml.etree.ElementTree as ET
import json
from botocore.exceptions import NoCredentialsError, ClientError
from collections import defaultdict
import os # Useful for mocking file paths if needed

## Configuration: CHANGE THESE VALUES 
# Replace with your actual S3 bucket name and the key (path) to your XML file
BUCKET_NAME = 'your-s3-bucket-name' 
S3_KEY = 'path/to/your/hive_sample.xml'   
# ----------------------------------------

def xml_to_dict_raw(element):
    """
    Standard recursive function to convert XML ElementTree to a basic Python dictionary.
    This is used as an intermediate step to easily access attributes and structure.
    """
    d = {element.tag: {} if element.attrib else None}
    children = list(element)

    if children:
        dd = defaultdict(list)
        for child in children:
            dd[child.tag].append(xml_to_dict_raw(child)[child.tag])

        d_content = {}
        for k, v in dd.items():
            d_content[k] = v[0] if len(v) == 1 else v
        
        if d[element.tag] is None:
            d[element.tag] = d_content
        else:
            d[element.tag].update(d_content)

    if element.attrib:
        if d[element.tag] is None:
             d[element.tag] = {}
        # Store attributes directly without prefixing @ for easier access in the transformation step
        d[element.tag].update(element.attrib) 

    if d[element.tag] == {}:
        d[element.tag] = None

    return d

def transform_to_labelstudio_format(xml_data_dict):
    """
    Transforms the raw XML dictionary into the specific Label Studio JSON task format.
    
    Key Transformation:
    1. Extracts image dimensions and path.
    2. Converts absolute pixel coordinates (xtl, ybr, etc.) into normalized 
       percentages (x, y, width, height) relative to the image size (0 to 100).
    3. Restructures data into Label Studio's required 'predictions/result' format.
    """
    # 1. Get the root image data and dimensions
    image_element = xml_data_dict.get('image', {})
    
    # Use image name from XML to construct the expected Label Studio 'image' path
    image_name = image_element.get('name') 
    # NOTE: You may need to adjust the path '/data/local-files/?d=' based on your LS setup
    ls_image_path = f"/data/local-files/?d={image_name}" 
    
    width = float(image_element.get('width', 1))
    height = float(image_element.get('height', 1))
    
    # Collect all annotation elements (boxes and polygons)
    annotations = []
    
    # 2. Process all <box> elements (assuming rectanglelabels)
    # Ensure 'box' is treated as a list, even if only one exists
    box_list = image_element.get('box')
    if box_list is not None:
        if not isinstance(box_list, list):
            box_list = [box_list]

        for box in box_list:
            # Absolute pixel coordinates
            xtl = float(box.get('xtl', 0))
            ytl = float(box.get('ytl', 0))
            xbr = float(box.get('xbr', 0))
            ybr = float(box.get('ybr', 0))
            label = box.get('label')
            
            # Normalize to Label Studio's percentage format (0 to 100)
            normalized_x = (xtl / width) * 100.0
            normalized_y = (ytl / height) * 100.0
            normalized_w = ((xbr - xtl) / width) * 100.0
            normalized_h = ((ybr - ytl) / height) * 100.0
            
            # 3. Build the Label Studio result object
            result_item = {
                "value": {
                    "x": normalized_x,
                    "y": normalized_y,
                    "width": normalized_w,
                    "height": normalized_h,
                    "rotation": 0, # Assuming no rotation from the XML sample
                    "rectangleLabels": [label]
                },
                "from_name": "label", # Default name for the labeling config's control tag
                "to_name": "image",   # Default name for the object tag (Image in LS)
                "type": "rectanglelabels",
                # Note: Score is not in XML, so we omit or set to None/1.0
                # If you need a score field, uncomment and set a value:
                # "score": 1.0 
            }
            annotations.append(result_item)
            
    # 4. Construct the final Label Studio task structure
    ls_task = [
        {
            "data": {
                # This path needs to be correct for your Label Studio project configuration
                "image": ls_image_path 
            },
            "predictions": [
                {
                    # Assuming a single model/prediction run
                    "model_version": "xml_import_v1", 
                    "result": annotations
                }
            ]
        }
    ]
    
    return ls_task

def load_and_convert_s3_xml_to_labelstudio_json(bucket_name, s3_key):
    """
    Main function to load XML from S3 and convert it to Label Studio JSON format.
    """
    s3 = boto3.client('s3')
    
    try:
        # 1. Load XML content from S3
        print(f"-> Downloading s3://{bucket_name}/{s3_key}")
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        xml_content = response['Body'].read()
        
        # 2. Parse the XML content
        xml_root = ET.fromstring(xml_content)
        print("-> XML loaded and parsed successfully.")
        
        # 3. Convert the parsed XML tree to a raw Python dictionary
        raw_xml_dict = xml_to_dict_raw(xml_root)
        
        # 4. Transform the raw dictionary into the Label Studio task structure
        labelstudio_task_data = transform_to_labelstudio_format(raw_xml_dict)
        
        # 5. Convert the dictionary list to a formatted JSON string
        json_output = json.dumps(labelstudio_task_data, indent=4)
        print("-> Conversion to Label Studio JSON format complete.")
        
        return json_output

    except NoCredentialsError:
        print("\n[ERROR] AWS credentials not found. Please configure them.")
        return None
    except ClientError as e:
        print(f"\n[ERROR] An S3 client error occurred: {e}")
        return None
    except Exception as e:
        print(f"\n[ERROR] An unexpected error occurred: {e}")
        return None

# --- Main Execution Block ---

if __name__ == "__main__":
    json_data = load_and_convert_s3_xml_to_labelstudio_json(BUCKET_NAME, S3_KEY)

    if json_data:
        print("\n" + "="*20 + " Final Label Studio JSON Output " + "="*20)
        print(json_data)
        print("="*64)
    else:
        print("\nProcess failed. Check error messages above.")
