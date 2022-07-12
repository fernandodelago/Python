# coding: utf-8
'''
Read files in a specific container in Azure Storage
'''
from azure.storage.blob import BlobServiceClient, ContainerClient
import os

connection_string = "connection_string"
service = BlobServiceClient.from_connection_string(conn_str=connection_string)

v_container_name = "container_name"
local_path = 'c:\\my_folder_to_write\\'
local_file_name = '.txt'
var_filename_from_blob = 'my_file_on_azure_storage.csv'

# set local to save the download file
download_file_path = os.path.join(local_path, str.replace(local_file_name ,'.txt', 'DOWNLOAD.csv'))

# get container information
container_client = service.get_container_client(v_container_name)

# get list of blobs(files inside container)
var_container_selected = container_client.list_blobs()

for item_found in var_container_selected:
    v_item_found = str(item_found.name).lower().replace("'","") # remove simple quotes

    if v_item_found == var_filename_from_blob.lower():
        print("\nDownloading blob to \n\t" + download_file_path)
        with open(download_file_path, "wb") as download_file:
            download_file.write(container_client.download_blob(item_found.name).readall())

print('###---- END PROGRAM ----###')