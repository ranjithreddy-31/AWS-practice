from s3tos3 import copy_files
def main():
    # Define source and destination S3 locations
    source_bucket = 'gluebkt'
    source_prefix = 'inbound/'
    destination_bucket = 'gluebkt'
    destination_prefix = 'outbound/'

    # Copy files from source to destination
    copy_files(source_bucket, source_prefix, destination_bucket, destination_prefix)

if __name__ == '__main__':
    main()
