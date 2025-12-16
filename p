Hey team,

I’ve written the code to move images from the Hive S3 bucket to the DataSci Share bucket, but it’s failing due to S3 permission issues, not the code itself.

We tried:

Downloading the files via SageMaker → not working

Directly copying from Hive bucket to DataSci Share bucket → not working

Even UI/console access to the Hive bucket isn’t available

Looks like the IAM role/user used by SageMaker/DataSci Share doesn’t have access to the Hive bucket.

We’ll need these permissions enabled on the Hive bucket:

s3:GetObject

s3:HeadObject

Once those are in place, the current code should work as-is. Let me know when access is granted and I’ll rerun it.

Thanks!




Hi team,

I’ve implemented the code to transfer images from the Hive S3 bucket to the DataSci Share S3 bucket. The logic is working correctly, but the job is failing due to S3 access permission issues, not code-related problems.

What we tried:

Downloading objects from the Hive bucket into SageMaker → fails

Direct S3-to-S3 copy from Hive bucket to DataSci Share bucket → fails

Accessing the Hive bucket via AWS Console/UI → no access

All attempts fail with access errors, which confirms this is a permission issue.

Root cause:
The IAM role/user being used by SageMaker / DataSci Share does not currently have access to the Hive bucket.

Permissions required on the Hive bucket:

s3:GetObject

s3:HeadObject
(for the IAM role/user used by SageMaker / DataSci Share)

Once these permissions are granted for the specific Hive bucket (and prefix if applicable), the existing code should work without any changes.

Please let me know once access is enabled, and I’ll re-run the job immediately.

Thanks!
