import boto3
from datetime import datetime
import os

def s3Client():
    # Configure AWS credentials
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID','AKIAQQ2ZKLT4SVQ3GBHF')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY','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')
    
    # Set up S3 client
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    return s3

def upload_file_to_s3(local_file_path, bucket_name, s3_folder_name, s3_file_name):
    """
    Uploads a file to an S3 bucket, creating a new folder in the bucket if it doesn't exist.
    
    Parameters:
    - local_file_path: The local path of the file to upload.
    - bucket_name: The name of the S3 bucket.
    - s3_folder_name: The name of the folder to create in the S3 bucket.
    - s3_file_name: The name of the file in the S3 bucket.
    """

    s3 = s3Client()

    # Use the S3 client for operations
    try:
        # Create the folder in S3 if it doesn't exist
        s3.put_object(Bucket=bucket_name, Key=f'{s3_folder_name}/')

        # Upload the file to S3
        s3.upload_file(local_file_path, bucket_name, f'{s3_folder_name}/{s3_file_name}')
        print(f'File {s3_file_name} uploaded to S3 bucket {bucket_name} in folder {s3_folder_name}')
    except Exception as e:
        print(f'Error uploading file to S3: {e}')

def list_s3_folders(bucket_name):
    # Create an S3 client
    s3 = s3Client()

    # List objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')

    # Extract folders (common prefixes) from the response
    folders = [prefix.get('Prefix') for prefix in response.get('CommonPrefixes', [])]

    # Iterate through response continuation tokens to list all folders
    while response.get('NextContinuationToken'):
        response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/', ContinuationToken=response['NextContinuationToken'])
        folders.extend([prefix.get('Prefix') for prefix in response.get('CommonPrefixes', [])])
    
    for folder in folders:
        print(f'folder name: {folder}')

    return folders

def list_s3_folder_contents(bucket_name, latest_folder):
    s3 = s3Client()

    # List objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix = latest_folder)

    # Extract files (objects) from the response
    files = [obj.get('Key') for obj in response.get('Contents', []) if not obj['Key'].endswith('/')]

    # Iterate through response continuation tokens to list all files
    while response.get('NextContinuationToken'):
        response = s3.list_objects_v2(Bucket=bucket_name, ContinuationToken=response['NextContinuationToken'])
        files.extend([obj.get('Key') for obj in response.get('Contents', [])])

    print(files)
    return files

def read_file_from_s3(bucket_name, file_key):
    # Create an S3 client
    s3 = s3Client()

    try:
        # Get the object from the S3 bucket
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        
        # Read and return the contents of the file
        contents = response['Body'].read().decode('utf-8')
        print(contents, type(contents))
        return contents
    except Exception as e:
        print("Error reading file from S3:", str(e))
        return None
    
# Example usage
if __name__ == "__main__":
    date = datetime.now().strftime('%Y%m%d')
    local_file_path = 'temp.txt'
    bucket_name = 'awspracbkt'
    s3_folder_name = f'NPI_{date}'
    s3_file_name = f'data_on_{date}.txt'

    #upload_file_to_s3(local_file_path, bucket_name, s3_folder_name, s3_file_name)

    latest_folder = max(list_s3_folders(bucket_name))

    files = list_s3_folder_contents(bucket_name, latest_folder)

    read_file_from_s3(bucket_name, files[0])

