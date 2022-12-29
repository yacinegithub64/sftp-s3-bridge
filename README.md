SFTP-S3 Bridge


1. Introduction

This project provides an SFTP-S3 bridge, which allows users to access the contents of an Amazon S3 bucket as if it were an SFTP server. It is implemented using the paramiko library for SFTP and the boto3 library for S3.


2. Requirements

    Python 3.6 or later
    boto3
    paramiko

3. Installation

    Clone the repository:

git clone https://github.com/yacinegithub64/sftp-s3-bridge.git



4. Configuration

To configure the SFTP-S3 bridge, create a configuration file in INI format with the following options:
SFTP

    host_key: path to the private key file for the SFTP server
    host_key_alg: algorithm used for the host key (e.g. ssh-rsa)
    listen_address: address for the SFTP server to listen on (e.g. 0.0.0.0)
    listen_port: port for the SFTP server to listen on (e.g. 2222)
    error_log_file: path to the error log file
    error_log_enabled: whether to enable error logging (True or False)
    access_log_file: path to the access log file
    access_log_enabled: whether to enable access logging (True or False)

S3

    aws_access_key_id: The AWS access key ID used to authenticate with the S3 service.
    aws_secret_access_key: The AWS secret access key used to authenticate with the S3 service.
    region_name: The region of the S3 bucket.
    bucket: The name of the S3 bucket.
    
User options

For each user that you want to allow to connect to the SFTP server, create a section in the configuration file with the following format: User <username>. The following options are available:

    username: The username of the user.
    password: The password of the user.
  
Running

To start the SFTP server, run the following command:

python main.py --config config.ini
