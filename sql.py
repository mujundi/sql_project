#!/usr/bin/env python

'''

Musa Jundi

SQL Project

This script does essentially the same thing as my Python Module Project script, but instead of
retrieving data from a file, it retrieves data from a database. It also takes in a new CSV file
and inserts the new entries into the 'ozone_data' table in the 'ozone' database.

'''

import csv
from sqlalchemy import *

# Connect to database
Database = "ozone"
User = "musa"
Password = "password"
Host = "localhost"
DBString = "mysql+pymysql://" + User + ":" + Password + "@" + Host + "/" + Database
e = create_engine(DBString)
DBInfo = MetaData(e)
data = Table('ozone_data', DBInfo, autoload=True)

print("Successfully connected to database.")

# Read new data entries from local csv file
with open('C:/Users/Musa/Downloads/project/New_Ozone.csv', newline='') as InFile:

	reader = csv.reader(InFile,delimiter=",")

	# Iterate through file, inserting the data entries into the 'ozone_data' table row by row
	print("Reading data from file...")
	for row in reader:
	
		insert = data.insert().values(SITE_ID = row[0], DATE_TIME = row[1],
										   OZONE = row[2], QA_CODE = row[3],
										   UPDATE_DATE = row[4])
		e.execute(insert)
	
print("All new entries added to ozone_data.")
print("Executing query...")
# Select a substring of the DATE_TIME column (year), and calculate average ozone concentrations grouped by year
query = '''SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(DATE_TIME,' ',1),'/',-1) AS year, AVG(OZONE) AS ozone_avg
		FROM ozone_data 
		GROUP BY year;'''
		
result = e.execute(query)


print("Printing results...\n\n")
# Print results from the 'result' object with methods similar to the dict methods '.keys()' and '.values()'
print(result.keys())
for row in result:
	print(row.values())		

e.dispose()
InFile.close()
