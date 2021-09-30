from __future__ import print_function

import mysql.connector
from mysql.connector import errorcode

dbName = 'Stations'

TABLES = {}

CREATE TABLE IF NOT EXISTS "time" (
  "date" DATETIME NOT NULL,
  "dayofweek" INT NOT NULL,
  "hour" INT NOT NULL,
  "minute" INT NOT NULL,
  "seconds" INT NOT NULL,
  PRIMARY KEY ("date")

) ENGINE=InnoDB;



CREATE TABLE IF NOT EXISTS "flow" (

  "flowid" INT NOT NULL AUTO_INCREMENT,
  "date" DATETIME NOT NULL,
  "flow1" INT NOT NULL,
  "flow2" INT NOT NULL,
  "flow3" INT NOT NULL,
  "flow3" INT NOT NULL,
  "flow3" INT NOT NULL,
  "flowtotal" INT NOT NULL,
  PRIMARY KEY ("flowid") 
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS "mph" (

  "mphid" INT NOT NULL AUTO_INCREMENT,
  "date" DATETIME NOT NULL,
  "mph1" INT NOT NULL,
  "mph2" INT NOT NULL,
  "mph3" INT NOT NULL,
  "mph4" INT NOT NULL,
  "mph5" INT NOT NULL,
  PRIMARY KEY ("mphid") 
) ENGINE=InnoDB;


CREATE TABLE IF NOT EXISTS "ocuppancy" (
  "ocuppancyid" INT NOT NULL AUTO_INCREMENT,
  "date" DATETIME NOT NULL,
  "ocuppancy1" INT NOT NULL,
  "ocuppancy2" INT NOT NULL,
  "ocuppancy3" INT NOT NULL,
  "ocuppancy4" INT NOT NULL,
  "ocuppancy5" INT NOT NULL,
  PRIMARY KEY ("ocuppancyid")
)ENGINE=InnoDB;



 
conn = mysql.connector.connect(host='localhost', user='root')
cur = conn.cursor()
    

"""cnx = mysql.connector.connect(user='root')
cursor = cnx.cursor()"""

def create_database(cur):
    try:
        cur.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(dbName))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

    try:
        cur.execute("USE {}".format(dbName))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(dbName))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cur)
            print("Database {} created successfully.".format(dbName))
            conn.database = dbName
        else:
            print(err)
            exit(1)

    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cur.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

    cur.close()
    conn.close()



if __name__ == "__main__":
    create_database(cur)
    
