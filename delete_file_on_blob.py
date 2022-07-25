# Databricks notebook source
from azure.storage.blob import BlobServiceClient
import os
import pandas as pd
from pyspark.sql.types import StructField, StructType, StringType

connection_string = "<connection_string>"
service = BlobServiceClient.from_connection_string(conn_str=connection_string)

# COMMAND ----------

v_container_name = "folder_name_on_blob_service"
#local_path = 'c:\\work\\'
#local_file_name = '.txt'
#var_filename_from_blob = 'GLIM_Delicias_Order_66.csv'

# COMMAND ----------

container_client = service.get_container_client(v_container_name)

# COMMAND ----------

# criando a lista de arquivos NJM_GLIM e GLIMS
ct = 0
ct2 = 0
ct_glims = 0
list_njm = []
list_glim = []
def insere_lista(nome_lista, item):
    nome_lista.append(str(item.name))

for item_blob in container_client.list_blobs():
    # find a specific string in the names of all items on a blob
    if 'NJM_GLIM20' in str(item_blob.name).upper():
        insere_lista(list_njm, item_blob)
        ct += 1
        if ct < 5:
            print(str(item_blob.name))
    elif 'GLIM20' in str(item_blob.name).upper() and 'delicia' not in str(item_blob.name).lower():
        insere_lista(list_glim, item_blob)
        ct2 += 1
        if ct2 < 5:
            print(str(item_blob.name))

print('# ct NJM ---> ' + str(ct))
print('# ct2 Glim -> ' + str(ct2))

# COMMAND ----------

# delete to Glim
ct = 0
ct2 = 0
for arq_no_blob in container_client.list_blobs():
    if str(arq_no_blob.name) in list_glim:
        ct += 1
        blobFile = service.get_blob_client(v_container_name, arq_no_blob.name)
        blobFile.delete_blob()
    elif str(arq_no_blob.name) in list_njm:
        ct2 += 1
        blobFile = service.get_blob_client(v_container_name, arq_no_blob.name)
        blobFile.delete_blob()
print('# Excluidos NJM  -> ' + str(ct))
print('# Excluidos Glim -> ' + str(ct2))
