import json
import os
import psycopg2
import getpass

print("Welcome to postgresToJSON, You'll need to run this program for EACH DATABASE whose information you want to dump\n")
print("Please keep your DATABASE NAME, USER NAME, PASSWORD, PORT and HOST ready!")

database = input("Please enter the name of the DATABASE you want to connect to: ").strip()
user = input("Please enter the USER who will connect: ").strip()
password = getpass.getpass("Please enter the PASSWORD for " + user + ": ")
hostname = input("Please enter the HOST url: ").strip()
port = input("Please enter the PORT: ").strip()

conn = psycopg2.connect(
   database = database, user = user, password = password, host = hostname, port = port
)
cursor = conn.cursor()

cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
allTables = cursor.fetchall()

os.makedirs(database)

print("Beginning iteration for Database "+ database + "...")
cursor.close()
conn.close()

for table in allTables:
   conn = psycopg2.connect(
      database = database, user = user, password = password, host = hostname, port = port
   )
   cursor = conn.cursor()
   tableName = table[0].strip()
   print("Processing '" + tableName + "'...\n")
   fileObj  = open(database + "/"+tableName+".json", "w+")
   
   try:
      cursor.execute("select json_agg(t) FROM (SELECT * from public.\"%s\") t"%tableName)
      try:
         data = cursor.fetchall()
         for row in data:
            fileObj.write((json.dumps(row))[1:-1])
      except: print("error")
   except:
      fileObj = open(database + "/"+tableName+"ERROR.json", "w+")
      fileObj.write("THIS TABLE DATA HAD AN ERROR.")
      print("THIS TABLE DATA HAD AN ERROR.")
   finally:
      fileObj.close()
      print(tableName + " written in" + tableName + ".json.\n\n")

print("Done.")