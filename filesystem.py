import boto3
import paramiko

class S3Filesystem(paramiko.SFTPDirectory):
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, bucket):
        # Initialize the S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.bucket = bucket

    def open(self, path, flags, attr):
        """Open the specified file.

        This method translates the SFTP `open` command to an S3 `get_object`
        command, and returns an SFTPFile object that can be used to read or
        write the file.

        Args:
            path (str): The path of the file to open.
            flags (int): The flags specifying the mode in which to open the
                file (e.g. read-only, write-only, etc.).
            attr (SFTPAttributes): The attributes of the file.

        Returns:
            An SFTPFile object representing the opened file.
        """
        # Get the object from S3
        response = self.s3_client.get_object(Bucket=self.bucket, Key=path)
        # Create an SFTPFile object to represent the file
        sftp_file = S3SFTPFile(self, path, flags, attr, response['Body'])
        return sftp_file

    def list_folder(self, path):
    try:
        response = self.s3_client.list_objects(Bucket=self.bucket, Prefix=path)
        files = []
        for obj in response['Contents']:
            file_info = paramiko.SFTPFileInfo()
            file_info.filename = obj['Key']
            file_info.st_size = obj['Size']
            file_info.st_mtime = int(obj['LastModified'].timestamp())
            file_info.st_mode = paramiko.S_IFREG if obj['ContentType'] != 'application/x-directory' else paramiko.S_IFDIR
            files.append(file_info)
        return files
    except Exception as e:
        raise paramiko.SFTPError(paramiko.SFTP_FAILURE, f'Error listing folder: {e}')
        
    def lstat(s3_client, bucket, path):
    try:
        obj = s3_client.head_object(Bucket=bucket, Key=path)
        attributes = paramiko.SFTPAttributes()
        attributes.filename = obj['Key']
        attributes.st_size = obj['ContentLength']
        attributes.st_mtime = int(obj['LastModified'].timestamp())
        attributes.st_mode = paramiko.S_IFREG if obj['ContentType'] != 'application/x-directory' else paramiko.S_IFDIR
        return attributes
    except Exception as e:
        raise paramiko.SFTPError(paramiko.SFTP_FAILURE, f'Error getting file info: {e}')
        
        
    def remove(self, path):
    try:
        self.s3_client.delete_object(Bucket=self.bucket, Key=path)
    except Exception as e:
        raise paramiko.SFTPError(paramiko.SFTP_FAILURE, f'Error deleting file: {e}')


       
