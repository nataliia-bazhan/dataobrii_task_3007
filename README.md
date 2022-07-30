# dataobrii_task_3007
Export data from Couchbase 'Travel Sample', add a new column and update csv file.

*main.py* contains a step-by-step script which:
- connects to the database
- retrieves all records from the 'travel-sample' and saves them in csv.
- adds new column 'testColumn' to all records in the database and checks the result
- updates csv file 

*database.py* contains TravelSample class with a connection to the database and all necessary queries. 
*cluster_credentials.py* contains information needed to connect to the database.
