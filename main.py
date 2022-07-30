from databases import TravelSample
import pandas as pd

# connect to database
db = TravelSample()

# save all records to csv file
file = 'travel-sample.csv'
df = db.select_all()
df.to_csv(file)

# add new column with vales o database
c_name = 'testColumn'
c_value = 'testValue'
success = db.add_column(c_name, c_value)

# updating csv file
c_df = db.select_cloumn(c_name)
# add c_df values of column to the existing csv
old_df = pd.read_csv(file)
new_df = pd.merge(old_df, c_df, on='id')
new_df.to_csv(file)

# delete new column from database
success = db.delete_column(c_name)
