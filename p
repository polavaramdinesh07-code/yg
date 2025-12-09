import boto3
import xml.etree.ElementTree as ET
from botocore.exceptions import NoCredentialsError, ClientError

# --- S3 Configuration ---
BUCKET_NAME = 'your-s3-bucket-name' # Replace with your S3 bucket name
S3_KEY = 'path/to/your/file.xml'   # Replace with the full path/key of your XML file

def load_xml_from_s3(bucket_name, s3_key):
    """
    Loads an XML file from S3, reads its content, and returns the parsed 
    ElementTree root object.
    """
    s3 = boto3.client('s3')
    
    try:
        # 1. Get the object from S3
        print(f"Attempting to download s3://{bucket_name}/{s3_key}")
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        
        # 2. Read the content (which is a byte stream)
        xml_content = response['Body'].read()
        
        # 3. Parse the XML content from the bytes
        # ET.fromstring expects a string or bytes/bytearray
        root = ET.fromstring(xml_content)
        
        print("Successfully loaded and parsed XML file.")
        return root
    
    except NoCredentialsError:
        print("Error: AWS credentials not found. Ensure your environment is configured (e.g., AWS CLI, environment variables).")
        return None
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Error: The key '{s3_key}' does not exist in bucket '{bucket_name}'.")
        elif e.response['Error']['Code'] == 'AccessDenied':
            print(f"Error: Access denied. Check your IAM permissions for bucket '{bucket_name}'.")
        else:
            print(f"An AWS client error occurred: {e}")
        return None
    except ET.ParseError as e:
        print(f"Error: Failed to parse XML content. Is the file well-formed? Details: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

# --- Usage Example ---
xml_root = load_xml_from_s3(BUCKET_NAME, S3_KEY)

if xml_root is not None:
    # Example: Print the root tag and find a specific element
    print(f"\nRoot Tag: {xml_root.tag}")
    
    # You can now process the XML data, for example:
    # all_images = xml_root.findall('image') # Assuming 'image' is a child tag
    # print(f"Found {len(all_images)} image elements.")
