import os
import configparser
import paramiko
import boto3

from filesystem import S3Filesystem

def main():
    # Parse the command line arguments
    parser = argparse.ArgumentParser(description='SFTP-S3 bridge')
    parser.add_argument('--config', required=True, help='Path to config file')
    args = parser.parse_args()

    # Load the config file
    config = configparser.ConfigParser()
    config.read(args.config)

    # Get the SFTP and S3 config options
    sftp_config = config['SFTP']
    s3_config = config['S3']

    # Read the list of users and passwords from the config file
    users = []
    for section in config.sections():
        if section.startswith('User '):
            username = config[section]['username']
            password = config[section]['password']
            users.append((username, password))

    # Create the S3 filesystem
    s3_filesystem = S3Filesystem(s3_config['aws_access_key_id'], s3_config['aws_secret_access_key'],
                                 s3_config['region_name'], s3_config['bucket'])

    # Create the SFTP server
    server = SFTPServer(sftp_config['host_key'], sftp_config['host_key_alg'], sftp_config['listen_address'],
                        sftp_config['listen_port'], sftp_config['error_log_file'], sftp_config['error_log_enabled'],
                        sftp_config['access_log_file'], sftp_config['access_log_enabled'], s3_filesystem, users)

    # Start the server
    server.start()

if __name__ == '__main__':
    main()
