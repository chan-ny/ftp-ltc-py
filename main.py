from ftplib import FTP

ftp_server = '172.28.26.189'
ftp_username = 'administrator'
ftp_password = 'Isd#$%2023'

try:
    # Connect to the FTP server
    ftp = FTP()
    ftp.connect(host=ftp_server, port=2121, timeout=30000)
    ftp.login(user=ftp_username, passwd=ftp_password)
    print(f"Connected to {ftp_server}")
    
    # Use nlist to get a list of files in the current directory
    ftp.cwd('/CDR_OCS/RAWCDR/OCS_ZIP')
    all_files = ftp.nlst()
    print(all_files)
    
    # prefix = 'tplus_pps_cbs_cdr_vou_20240919'
    # matching_files = [file for file in all_files if file.startswith(prefix)]
    
    # print(f"Files starting with '{prefix}': {len(matching_files)}")
    # # List files in the current directory
    # ftp.retrlines('LIST')

    # # Download a file
    # filename = 'example.txt'
    # with open(filename, 'wb') as local_file:
    #     ftp.retrbinary(f'RETR {filename}', local_file.write)
    # print(f"Downloaded {filename}")

    # # Upload a file
    # filename = 'upload_file.txt'
    # with open(filename, 'rb') as file_to_upload:
    #     ftp.storbinary(f'STOR {filename}', file_to_upload)
    # print(f"Uploaded {filename}")

finally:
    # Close the FTP connection
    ftp.quit()
    print("Connection closed")
