import os
import configparser
import paramiko
import boto3

from filesystem import S3Filesystem

def main():
    # Read the config file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Parse the SFTP and logging configuration options
    sftp_address = config['SFTP']['address']
    sftp_port = int(config['SFTP']['port'])
    sftp_username = config['SFTP']['username']
    sftp_password = config['SFTP']['password']
    error_log_file = config['Logging']['error_log']
    error_log_enabled = config['Logging']['error_log_enabled'] == 'true'
    access_log_file = config['Logging']['access_log']
    access_log_enabled = config['Logging']['access_log_enabled'] == 'true']

    # Create an S3 client
    s3_address = config['S3']['address']
    aws_access_key_id = config['S3']['access_key_id']
    aws_secret_access_key = config['S3']['secret_access_key']
    bucket = config['S3']['bucket']
    s3_filesystem = S3Filesystem(s3_address, aws_access_key_id, aws_secret_access_key)

    # Start the SFTP server
    server = paramiko.server.ServerInterface()
    server.s3_filesystem = s3_filesystem
    server.error_log_file = error_log_file
    server.error_log_enabled = error_log_enabled
    server.access_log_file = access_log_file
    server.access_log_enabled = access_log_enabled

    try:
        # Create an SFTP server and start listening for connections
        ssh_server = paramiko.server.SSHServer()
        ssh_server.add_global_request_handler(paramiko.SFTPServer.from_server, paramiko.SFTPServer)
        ssh_server.start_server(server=server)
    except Exception as e:
        # Log the error and exit
        if server.error_log_enabled:
            with open(server.error_log_file, 'a') as f:
                f.write(f'Error starting SFTP server: {e}\n')
        return

if __name__ == '__main__':
    main()
