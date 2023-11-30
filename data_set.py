import time
import pandas as pd
import sqlite3 as sq
import numpy as np
import csv
from faker import Faker
start = time.time()

def get_csv_Dataset(size, file_name):
    my_name_list = [Faker().name() for _ in range(50)]
    my_emails = [Faker().email() for _ in range(50)]
    df = pd.DataFrame()
    df['id'] = np.random.randint(10000, 99999, size)
    df['name'] = np.random.choice(my_name_list, size)
    df['age'] = np.random.randint(20, 60, size)
    df['email'] = np.random.choice(my_emails, size)
    df['rate'] = np.random.uniform(1, 0, size)
    df.to_csv(file_name, index=False)



def csv_To_Database(file_path,chunck_size):
    try:
        csv_file = pd.read_csv(file_path, index_col=False,chunksize=chunck_size)
        FileName = file_path[0:-3] + "db"
        con = sq.connect(FileName)
        for chunk in csv_file:
            df=pd.DataFrame(chunk)
            data_base = df.to_sql('database', con, index=False,if_exists="append")
    except sq.Error as error:
        print(error)
    finally:
        con.commit()
        con.close()

# get_csv_Dataset(2000,"ramy.csv")
# csv_To_Database("ramy.csv",100)

if __name__ == '__main__':
    end = time.time()
    print(round((end-start),8),'sec')