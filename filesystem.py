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
    """List the contents of the specified folder.

    This method translates the SFTP `list_folder` command to an S3
    `list_objects` command, and returns the contents of the specified
    folder as a list of SFTPFileInfo objects.

    Args:
        path (str): The path of the folder to list.

    Returns:
        A list of SFTPFileInfo objects representing the contents of the
        folder.
    """
    try:
        # List the objects in the specified S3 bucket and prefix
        response = self.s3_client.list_objects(
            Bucket=self.bucket, Prefix=path)
        # Convert the S3 objects to SFTPFileInfo objects
        files = [self._s3_obj_to_sftp_file(obj) for obj in response['Contents']]
        return files
    except Exception as e:
        # Log the error and return an empty list
        if self.error_log_enabled:
            with open(self.error_log_file, 'a') as f:
                f.write(f'Error listing folder "{path}": {e}\n')
        return []

       
