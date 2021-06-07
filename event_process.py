import pytz
import keys as key
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas

"""
    Access contents of the Azure blob storage. Write the name of the blob uploaded to the text file.
    Retrieve the most recently uploaded block blob URL.

    @param account_name: name of the Azure Storage account (can change this parameter in the driver code)
    @param container_name: string name of container (can change this parameter in the driver code)
    @return dictionary that has name of the blob as a key, and SAS URL of the blob as the value
"""
def retrieve_blob(account_name, container_name):
    # Instantiate a BlobServiceClient using a connection string
    blob_service_client = BlobServiceClient.from_connection_string(key.CONNECTION_STRING_1)
    container_client = blob_service_client.get_container_client(container_name)

    # Collect all the blob names that are past June 1, 2021
    utc = pytz.UTC
    last_date = utc.localize(datetime(2021, 6, 1))
    blob_name_date_dictionary = dict()
    for blob in container_client.list_blobs():
        if blob.last_modified > last_date:
            blob_name_date_dictionary[blob.name] = generate_sas(account_name, container_name, blob.name)
    
    # Write the name of the blob file to blob_list.txt
    with open('blob_list.txt', 'r+') as file:
        for blob_name in blob_name_date_dictionary.keys():
            for line in file:
                if blob_name in line: # If the name of the blob already exists in the file, do not write it
                    break
            else:
                file.write(blob_name)
                file.write("\n")
    
    return blob_name_date_dictionary

"""
    Generates the SAS token that needs to be at the end of a blob storage URL for download.

    @param account_name: name of the Azure Storage account (can change this parameter in the driver code)
    @param container_name: string name of container (can change this parameter in the driver code)
    @param blob_name: name of blob that needs to be downloaded
    @return URL of the most recently uploaded blob to the storage container with the SAS token at the end
"""
def generate_sas(account_name, container_name, blob_name):
    sas = generate_blob_sas(account_name = account_name, 
                    account_key = key.KEY_1, 
                    container_name = container_name,
                    blob_name = blob_name, 
                    permission = BlobSasPermissions(read = True),
                    expiry = datetime.utcnow() + timedelta(hours = 2))
    sas_url = 'https://' + account_name + '.blob.core.usgovcloudapi.net/' + container_name + '/' + blob_name + '?' + sas
    return sas_url

"""
    Run the clustering and analysis process on the blob.
"""
def run_process():
    return

"""
    Once the blob has been processed, remove it from the list.
"""
def delete_blob_from_list():
    return

if __name__ == "__main__":
    storage_account = key.STORAGE_ACCOUNT
    container_name = key.CONTAINER_NAME
    blob_name_date_dictionary = retrieve_blob(storage_account, container_name)