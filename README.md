### cron Job I

Check the Azure Storage container for recent uploads at the specifie date. Write the name of the file unprocessed file to the text file.

### cron Job II

Check the text file to grab the name of the blob. Run this blob through the process. Once processed, delete the name from the file.