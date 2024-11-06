import ftplib
import os
import time
from datetime import datetime

# FTP server configuration
FTP_HOST = "172.28.12.170"
FTP_USER = "ltcisd"
FTP_PASS = "ISD$$123"
REMOTE_DIR = "/EPC2/SGW-PGWCDR/ap1/second/pgwcdr"
LOCAL_DIR = "log_dir"

# File to log downloaded files
LOG_FILE = "log-download.txt"
# Track the last date the log file was updated
last_date = datetime.now().date()

# Read existing downloaded files from log
def read_log():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            return set(f.read().splitlines())
    return set()

# Append a filename to the log file
def append_to_log(filename):
    with open(LOG_FILE, "a") as f:
        f.write(filename + "\n")

# Clear the log file for a new day
def clear_log():
    open(LOG_FILE, "w").close()

# Download new files from the FTP server
def download_new_files():
    downloaded_files = read_log()
    
    ftp_server = '172.28.12.170'
    ftp_username = 'ltcisd'
    ftp_password = 'ISD$$123'
    
    ftp = ftplib.FTP()
    ftp.connect(host=ftp_server, port=2124, timeout=30000)
    ftp.login(user=ftp_username, passwd=ftp_password)
    print(f"Connected to {ftp_server}")
    
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y%m%d")
    ftp.cwd(f'{REMOTE_DIR}/{formatted_datetime}')
    files = ftp.nlst() 

    for filename in files:
        if filename not in downloaded_files:
            local_file_path = os.path.join(LOCAL_DIR, filename)
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            with open(local_file_path, "wb") as local_file:
                ftp.retrbinary(f"RETR {filename}", local_file.write)
            print(f"Downloaded: {filename}")
            append_to_log(filename)



# Main loop to run the check every 5 minutes
while True:
    current_date = datetime.now().date()
    # Check if a new day has started
    if current_date != last_date:
        clear_log()
        print("New day detected. Cleared log file.")
        last_date = current_date

    download_new_files()
    time.sleep(300)  # 5 minutes in seconds
