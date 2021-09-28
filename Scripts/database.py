import pandas as pd
import psycopg2


median_columns = ["ID","weekday","hour","minute","second","flow1","occupancy1","mph1","flow2","occupancy2","mph2","flow3","occupancy3","mph3","flow4","occupancy4","mph4","flow5","occupancy5","mph5","totalflow"]
medians_table_query = '''CREATE TABLE median_sensors(
        Id INTEGER,
        weekday INTEGER,
        hour INTEGER,
        minute INTEGER,
        second INTEGER,
        flow1 FLOAT,
        occupancy1 FLOAT,
        mph1 FLOAT
        flow2 FLOAT,
        occupancy2 FLOAT,
        mph2 FLOAT
        flow3 FLOAT,
        occupancy3 FLOAT,
        mph3 FLOAT,
        flow4 FLOAT,
        occupancy4 FLOAT,
        mph4 FLOAT,
        flow5 FLOAT,
        occupancy5 FLOAT,
        mph5 FLOAT
        totalflow FLOAT
)'''

MEDIAN_TABLE_NAME = 'median_sensors'
DATA_SOURCE_ADDRES = 'C:/Users/user/Desktop/10Academy/week 11/180_median.csv'
DATABASE_BEZ = 'bez'
DATABASE_DEV = 'development'

USER = 'postgres'

def get_db_connection(database=DATABASE_DEV):
    try:
        print('Trying to connect to Database')
        connection = psycopg2.connect(user = postgres,
                                  password = "POS2021@sabi",
                                  host = "localhost",
                                  port = "5432",
                                  database = database)
        print('Connected.')
        return connection

    except:
        print("[ ERROR ] coudn't connect to databse")
        return None


def create_table(database, query):
    conn = get_db_connection(database)
    try:
        print('Creating A Table')
        
        cursor = conn.cursor()
        cursor.execute(query)    
        conn.commit()

        print('Table Created')
    except:
        print("[ ERROR ] can't exicute the query")
        return None
    
    finally:
        if(conn):
            conn.close()


def add_to_table(database, table_name, values):
    conn = get_db_connection(database)
    try:
        print(f"Adding New Data To {table_name} Table")

        cursor = conn.cursor()
        placeholder = ("%s, " * len(values))[:-2]
        cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholder})", values)    
        conn.commit()

        print('New Data Added.')
    
    except Exception as e:
        print("[ ERROR ] couldn't add to the table")
        print(e)
    
    finally:
        if(conn):
            conn.close()


def drop_table(database, table_name):
    conn = get_db_connection(database)
    try:
        cursor = conn.cursor()
        cursor.execute(f'DROP TABLE {table_name}')
    except Exception as e:
        print(f'Failed to drop {table_name}')
        print(e)


def clear_table(database, table_name):
    conn = get_db_connection(database)
    try:
        print('Clearning Table')
        
        cursor = conn.cursor()
        cursor.execute(f'DELETE FROM {table_name}')
        conn.commit()

        print('Table Cleared')
        
    except Exception as e:
        print(f"[ ERROR ] couldn't clear {table_name} table")
        print(e)
    

def add_to_table_from_dataset(source_path, table_name):
    
    conn = get_db_connection(DATABASE_DEV)
    cursor = conn.cursor()

    print("Setting up dataframe ...")
    df = pd.read_csv(source_path, sep=',',quotechar='\"', encoding='utf-8')
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    print("Generating query string from the data ...")
    placeholder = ("%%s, " * len(df.columns))[:-2]
    query  = ("INSERT INTO %s(%s) VALUES("+placeholder+")" )% (table_name, cols)
    
    try:
        print("Exicuting Querys")
        
        cursor.executemany(query, tuples)
        conn.commit()
        conn.close()

        print('Finished Exicuting Queries')

    except Exception as e:
        print(f'[ ERROR ] Failed to add data to {table_name} table')
        print(e)


# if __name__ == '__main__':
# i need to run thus files in the dag.

def main():
    create_table(DATABASE_DEV, median_table_query)
    clear_table(DATABASE_DEV, MEDIAN_TABLE_NAME)
    add_to_table_from_dataset(DATA_SOURCE_ADDRES,MEDIAN_TABLE_NAME)

# Function to check the proper insertion of data
def read_from_table(database, table):
    conn =get_db_connection(database)
    cursor = conn.cursor()
    cursor.execute(f'Select * from {table}')
    result = cursor.fetchall()

    print(result)
    conn.close()
# read_from_table(DATABASE_DEV, MEDIAN_TABLE_NAME)