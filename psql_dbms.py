import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # <-- ADD THIS LINE

class DBMS: 
   def __init__(self, user_info:str, port:int, dbn:str):
       """
       Get all data from a URL 
       :args: 
          user_info:str - connection info for database [user@ip:passwd] 
       :param: 
          db_user:str - user name 
          db_host:str - database ip 
          db_passwd:str - database password for user 
          db_port:str - database port 
          db_name:str - database name 
       """
       self.db_user = user_info.split("@")[0] 
       self.db_host = user_info.split("@")[-1].split(":")[0]
       self.db_passwd = user_info.split(":")[-1] 
       self.db_port = port 
       self.db_name = dbn 

   def connect_dbms(self)->psycopg2.extensions.cursor: 
      """
      Connect to database 
      :return: 
         connection to database 
      """
      conn = psycopg2.connect(host=self.db_host, port=self.db_port, user=self.db_user, password=self.db_passwd, dbname=self.db_name)
      conn.autocommit = True
      return conn.cursor()

