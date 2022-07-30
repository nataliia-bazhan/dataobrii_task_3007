from datetime import timedelta
import cluster_credentials as cc
import pandas as pd

from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException


class TravelSample:
    # class for operating with travel sample bucket
    table_name = "`travel-sample`"

    def __init__(self, username=cc.USERNAME, password=cc.PASSWORD, host=cc.HOST):
        # connect to database
        print('Connecting to database...\n')

        pa = PasswordAuthenticator(username, password)
        try:
            self.cluster = Cluster(host, ClusterOptions(pa))
            self.cluster.wait_until_ready(timedelta(seconds=5))
        except CouchbaseException as error:
            print(f"Could not connect to cluster. Error: {error}")
            raise


    def select_all(self, n=0):
        # returns data frame with all records or with n records if specified
        query = f"select * from {self.table_name}"
        if int(n) > 0:
            query += f" limit {int(n)}"
        result = self.cluster.query(query)
        df = pd.DataFrame(result)
        return df


    def select_cloumn(self, column_name):
        # returns data frame with all values of column
        query = f"select t.id, t.{column_name} from {self.table_name} as t"
        result = self.cluster.query(query)
        df = pd.DataFrame(result)
        return df


    def add_column(self, column_name, column_value="test_value"):
        # add new column with column_value to all records
        print(f'Adding column {column_name}...')

        query = f"""
                update {self.table_name} as t 
                set t.{column_name} = $value"""
        result = self.cluster.query(query, value=column_value).execute()

        # in case of success returns 1
        return self.check_if_column_exists(column_name)


    def delete_column(self, column_name):
        # deletes column from each record
        print(f'Deleting column {column_name}...')

        query = f"""
                update {self.table_name} as t 
                unset t.{column_name}"""
        result = self.cluster.query(query).execute()

        # in case of success returns 1
        return not self.check_if_column_exists(column_name)


    def check_if_column_exists(self, column_name):
        # checks if column indeed exists in every record
        query = f"""
                select tc.*, array_contains(tc.columnNames, '{column_name}') as columnExist 
                from (select distinct object_names(t) as columnNames, t.type from {self.table_name} as t) as tc
             """
        result = self.cluster.query(query)
        df = pd.DataFrame(result)
        success = df['columnExist'].all()

        # returns True only if all collections in cluster contain column
        print(f'Column {column_name} ' + ('exists' if success else 'does not exist') + '\n')
        return


    def save_to_csv(self, file, n):
        df = self.select_all(n)
        df.to_csv(file)



