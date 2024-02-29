import boto3

def copy_files(source_bucket, source_prefix, destination_bucket, destination_prefix):
    # Create S3 client
    s3 = boto3.client('s3')
    
    # List objects in the source bucket with the specified prefix
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
    
    # Check if there are any objects in the source bucket
    if 'Contents' in response:
        # Iterate over the objects in the source bucket
        for obj in response['Contents']:
            # Extract the key (filename) of the object
            key = obj['Key']
            
            # Construct the new key for the destination object
            new_key = key.replace(source_prefix, destination_prefix, 1)
            
            # Copy the object from the source bucket to the destination bucket
            s3.copy_object(
                Bucket=destination_bucket,
                CopySource={'Bucket': source_bucket, 'Key': key},
                Key=new_key
            )
            print(f"Copied object from {source_bucket}/{key} to {destination_bucket}/{new_key}")
    else:
        print("No objects found in the source bucket.")
