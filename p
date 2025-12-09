import boto3
import xml.etree.ElementTree as ET
import json
from botocore.exceptions import NoCredentialsError, ClientError
from collections import defaultdict

## Configuration: CHANGE THESE VALUES 
# Replace with your actual S3 bucket name and the key (path) to your XML file
BUCKET_NAME = 'your-s3-bucket-name' 
S3_KEY = 'path/to/your/hive_sample.xml'   
# ----------------------------------------

def xml_to_dict(element):
    """
    Recursively converts an XML ElementTree element into a Python dictionary.
    
    It handles:
    1. Attributes (prefixed with '@').
    2. Nested elements.
    3. Repeating sibling elements (stored as a list).
    4. Text content ('#text').
    """
    d = {element.tag: {} if element.attrib else None}
    children = list(element)

    if children:
        # Use defaultdict(list) to easily group repeating elements
        dd = defaultdict(list)
        for child in children:
            # Recursively convert child element
            dd[child.tag].append(xml_to_dict(child)[child.tag])

        # Convert defaultdict back to a regular dict
        d_content = {}
        for k, v in dd.items():
            # If a tag has only one instance, store it directly (not as a list)
            d_content[k] = v[0] if len(v) == 1 else v
        
        # Update the main dictionary with child content
        if d[element.tag] is None:
            d[element.tag] = d_content
        else:
            d[element.tag].update(d_content)

    if element.attrib:
        # Add attributes, often prefixed with '@' for clarity
        if d[element.tag] is None:
             d[element.tag] = {}
        d[element.tag].update(('@' + k, v) for k, v in element.attrib.items())

    # Handle simple text content
    if element.text and element.text.strip():
        text_content = element.text.strip()
        if d[element.tag] is None:
            # Simple element like <tag>value</tag>
            d[element.tag] = text_content
        else:
            # Mixed content (text + attributes/children)
            d[element.tag]['#text'] = text_content

    # Clean up empty dictionary if no children, attributes, or text were found
    if d[element.tag] == {}:
        d[element.tag] = None

    return d

# --- Main Workflow Function ---

def load_and_convert_s3_xml_to_json(bucket_name, s3_key):
    """
    Loads an XML file from S3, converts its content to a Python dictionary,
    and then returns a formatted JSON string.
    """
    # Initialize the S3 client. Boto3 will automatically look for AWS credentials.
    s3 = boto3.client('s3')
    
    try:
        # 1. Load XML content from S3
        print(f"-> Downloading s3://{bucket_name}/{s3_key}")
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        
        # The 'Body' is a StreamingBody object; read() gets the bytes
        xml_content = response['Body'].read()
        
        # 2. Parse the XML content bytes into an ElementTree root
        xml_root = ET.fromstring(xml_content)
        print("-> XML loaded and parsed successfully.")
        
        # 3. Convert the parsed XML tree to a Python dictionary
        xml_dict = xml_to_dict(xml_root)
        
        # 4. Convert the dictionary to a formatted JSON string
        # Use indent=4 for human-readable output
        json_output = json.dumps(xml_dict, indent=4)
        print("-> Conversion to JSON complete.")
        
        return json_output

    except NoCredentialsError:
        print("\n[ERROR] AWS credentials not found. Please ensure your environment is configured (e.g., AWS CLI, environment variables, or IAM role).")
        return None
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"\n[ERROR] Key '{s3_key}' does not exist in bucket '{bucket_name}'. Please verify the path.")
        else:
            print(f"\n[ERROR] An AWS client error occurred: {e}")
        return None
    except ET.ParseError as e:
        print(f"\n[ERROR] Failed to parse XML content. Ensure the file is well-formed. Details: {e}")
        return None
    except Exception as e:
        print(f"\n[ERROR] An unexpected error occurred: {e}")
        return None

# --- Main Execution Block ---

if __name__ == "__main__":
    # This calls the main function with the configured variables
    json_data = load_and_convert_s3_xml_to_json(BUCKET_NAME, S3_KEY)

    if json_data:
        print("\n" + "="*20 + " Final JSON Output " + "="*20)
        print(json_data)
        print("="*57)
    else:
        print("\nProcess failed. Check error messages above.")
