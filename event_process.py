import pytz
import keys as key
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas

"""
    Access contents of the Azure blob storage. Write the name of the blob uploaded to the text file.
    Retrieve the most recently uploaded block blob URL.

    @param account_name: name of the Azure Storage account (can change this parameter in the driver code)
    @param container_name: string name of container (can change this parameter in the driver code)
    @return URL of the most recently uploaded blob to the storage container with the SAS token at the end
    @return name of the blob that was most recently updated/uploaded
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
        if blob.last_modified < last_date:
            blob_name_date_dictionary[blob.name] = generate_sas(account_name, container_name, blob.name)

"""
    Generates the SAS token that needs to be at the end of a blob storage URL for download.

    @param account_name: name of the Azure Storage account (can change this parameter in the driver code)
    @param container_name: string name of container (can change this parameter in the driver code)
    @param blob_name: name of blob that needs to be downloaded
    @return URL of the most recently uploaded blob to the storage container with the SAS token at the end
"""
def generate_sas(account_name, container_name, blob_name):
    # Generate SAS token to place at the end of the URL of the last uploaded (modified) blob on the container
    sas = generate_blob_sas(account_name = account_name, 
                    account_key = key.KEY_1, 
                    container_name = container_name,
                    blob_name = blob_name, 
                    permission = BlobSasPermissions(read = True),
                    expiry = datetime.utcnow() + timedelta(hours = 2))
    sas_url = 'https://' + account_name + '.blob.core.usgovcloudapi.net/' + container_name + '/' + blob_name + '?' + sas
    return sas_url

def run_process():
    return

if __name__ == "__main__":
    STORAGE_ACCOUNT = 'weedsmedia'
    CONTAINER_NAME = 'weeds3d'

    # Access contents of the Azure Blob Storage
    retrieve_blob(STORAGE_ACCOUNT, CONTAINER_NAME)