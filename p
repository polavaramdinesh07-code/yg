import sagemaker
from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput
from sagemaker import get_execution_role
import datetime
import os

# --- ⚠️ S3 & Job Configuration (UPDATE THESE) ⚠️ ---
# 1. Bucket/Prefix containing your annotation JSON files
ANNOTATIONS_S3_URI = 's3://your-annotations-bucket/annotations_export/'
# 2. Bucket/Prefix containing your original images
IMAGES_S3_URI = 's3://your-source-image-bucket/all_images/'
# 3. Target S3 location for the cropped images
CROPS_OUTPUT_S3_URI = 's3://your-output-bucket/cropped_output/images/'

# Job settings
INSTANCE_TYPE = 'ml.m5.xlarge' # Recommended instance type for image processing
INSTANCE_COUNT = 1
PROCESSING_JOB_NAME = f"image-cropper-{datetime.datetime.now():%Y-%m-%d-%H-%M-%S}"
CROP_SIZE_PIXELS = 256 

# --- SageMaker Setup ---
try:
    role = get_execution_role()
except ValueError:
    print("WARNING: Could not retrieve execution role. Ensure you are running in SageMaker or define the role manually.")
    role = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_EXECUTION_ROLE_NAME' # Define your role ARN here
    
sagemaker_session = sagemaker.Session()
region = sagemaker_session.boto_region_name

# Use a standard Docker image that includes Python and common data science libraries (like SKLearn),
# which often includes common C libraries needed for OpenCV.
# If you encounter cv2 errors, you may need a custom image with libopencv-dev installed.
image_uri = sagemaker.image_uris.retrieve('python', region, '3.10') # Use a base Python image

# 1. Define the Processor
processor = ScriptProcessor(
    image_uri=image_uri,
    command=['python3'],
    role=role,
    instance_count=INSTANCE_COUNT,
    instance_type=INSTANCE_TYPE,
    max_runtime_in_seconds=3600 * 4 # 4 hours max runtime
)

# 2. Define Inputs and Outputs (Maps S3 -> Container Path)
inputs = [
    ProcessingInput(
        source=ANNOTATIONS_S3_URI, 
        destination=ANNOTATIONS_INPUT_DIR, # /opt/ml/processing/input/annotations
        input_name='annotations_input'
    ),
    ProcessingInput(
        source=IMAGES_S3_URI, 
        destination=IMAGES_INPUT_DIR, # /opt/ml/processing/input/images
        input_name='images_input'
    )
]

outputs = [
    ProcessingOutput(
        source=CROPS_OUTPUT_DIR, # /opt/ml/processing/output/crops
        destination=CROPS_OUTPUT_S3_URI, # The S3 path where crops will be saved
        output_name='cropped_images_output'
    )
]

# 3. Run the Job
print(f"Starting SageMaker Processing Job: {PROCESSING_JOB_NAME}")

processor.run(
    code='processing_script.py', 
    inputs=inputs,
    outputs=outputs,
    job_name=PROCESSING_JOB_NAME,
    arguments=['--crop-size', str(CROP_SIZE_PIXELS)],
    wait=True # Wait for job completion for immediate feedback
)

print(f"\n✨ Job finished successfully! Cropped images are available at: {CROPS_OUTPUT_S3_URI}")
