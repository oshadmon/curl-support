import argparse
import datetime 
import json 
import os
import requests
import time 

from file_io import FileIO
from psql_dbms import DBMS 

def validate_dirs(dir_name:str)->str: 
   """
   Validate directory exists, if not create it 
   :args: 
      dir_name:str - dir to check 
   :return: 
      dir with full path 
   """
   dir_name = os.path.expanduser(os.path.expandvars(dir_name))
   if not os.path.isdir(dir_name):
      try: 
         os.makedirs(dir_name) 
      except: 
         return False 
   return dir_name 

def convert_dict_to_json(result_set)->str: 
   return json.dumps(result_set)

def get_last_timestamp(user_info:str, port:int, dbn:str, table:str)->str: 
   """
   Get latest timestamp for table 
   :args: 
      user_info:str - database connection info 
      port:int - database connection port 
      dbn:str - logical datbaase 
      table:str - table with relevent info 
   :return: 
      latest timestamp 
   """
   dbms = DBMS(user_info, port, dbn)
   cur = dbms.connect_dbms()
   cur.execute("SELECT MAX(time) FROM %s;" % table)
   result = str(cur.fetchall()[0][0]).split(".")[0]
   cur.close()
   return result 

class GetData: 
   def __init__(self, url:str, db:str, query:str, prep_dir_name:str, ready_dir_name:str):
      """
      Get data from URL and store into files in query 
      :param: 
         self.url:str - URL to get data from 
         self.db:str - logical database to get data from 
         self.query:str - Query to get data 
         self.prep_dir_name:str - directory where data is prepped 
         self.ready_dir_name:str - directory where data is ready to be sent 
      """
      self.url = url 
      self.db  = db 
      self.query = query 
      self.prep_dir_name = prep_dir_name 
      self.ready_dir_name = ready_dir_name 
      self.fileio = FileIO(self.prep_dir_name, self.ready_dir_name) 

   def request_data(self)->dict: 
      """
      execute cURL command to get data 
      :args: 
         query:str - query to execute 
      :return: 
         result based on query
      """
      params = {'db': self.db, 'q': self.query} 
      print(params)
      print(requests.get(self.url, params=params))
      try: 
         return requests.get(self.url, params=params).json()
      except Exception as e:  
         print(e) 
         return False 

   def format_data(self, request_result:dict)->(str, str, str, list): 
      """
      Given results, get relevent information
      :args: 
         request_result:str - raw results
      :param: 
         data_set:list - list of formatted results 
         table_name:str - table to store data into (based on  data in JSON) 
         table_columns:str - list of columns 
      :return: 
         table name, initial timestamp and formatted data set 
      """
      data_set = [] 
      timestamp = '' 
      for key in list(request_result.keys()): 
         for row in request_result[key]: 
            table_name = row['series'][0]['name'] 
            table_columns = row['series'][0]['columns']
            for value in row['series'][0]['values']: 
               data = {} 
               for column in table_columns:
                  data[column] = value[table_columns.index(column)]
                  if data[column] is None: 
                     data[column] = ''
               if timestamp == '': 
                  timestamp = datetime.datetime.strptime(data['time'].split(".")[0], '%Y-%m-%dT%H:%M:%S').strftime('%Y_%m_%d_%H_%M_%S')
                  sensor_id = '%s_%s' % (data['host'], data['region']) 
               data_set.append(data) 

      return table_name, timestamp, sensor_id, data_set

      
   def execute_process(self): 
      request_result = self.request_data() 
      table_name, timestamp, sensor_id, results = self.format_data(request_result)
      if not self.fileio.check_if_file_exists(sensor_id, table_name):
         self.fileio.create_file(sensor_id, timestamp, table_name) 
      file_name = self.fileio.check_if_file_exists(sensor_id, table_name)
      for row in results: 
         self.fileio.write_to_file(file_name, convert_dict_to_json(row))
      if self.ready_dir_name != self.prep_dir_name:
         self.fileio.move_file(file_name, self.ready_dir_name) 

def main(): 
   """
   :positional arguments:
      user_info             connection to database
      port                  database port connection info
      dbn                   logical database name
      table                 table to get data of
   :optional arguments:
      -h, --help            show this help message and exit
      -u URL, --url URL     URL to get data from (default: http://trunoz.com:8086/query)
      -udb URL_DB, --url-db URL_DB Database for URL (default: PI_SENSOR)
      -pdn PREP_DIR_NAME, --prep-dir-name PREP_DIR_NAME Directory where data is prepped (default: /tmp)
      -rdn READY_DIR_NAME, --ready-dir-name READY_DIR_NAME Directorry where data is ready to be sent (default: /tmp)
   :sample command: 
      python3  $HOME/curl-support/get_data.py anylog@192.168.1.236:demo 5432 pi_sensor_mx_south humidity -pdn $HOME/AnyLog-Network/data/prep -rdn $HOME/AnyLog-Network/data/watch 
   """
   parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
   parser.add_argument('user_info',                type=str,   default='anylog@127.0.0.1:demo',                 help='connection to database') 
   parser.add_argument('port',                     type=int,   default=5432,                                    help='database port connection info') 
   parser.add_argument('dbn',                      type=str,   default='pi_sensor_mx_south',                    help='logical database name') 
   parser.add_argument('table',                    type=str,   default='',                                      help='table to get data of') 
   parser.add_argument('-u', '--url',              type=str,   default='http://trunoz.com:8086/query',          help='URL to get data from') 
   parser.add_argument('-udb', '--url-db',         type=str,   default='PI_SENSOR',                             help='Database for URL') 
   parser.add_argument('-pdn', '--prep-dir-name',  type=str,   default='/tmp',                                  help='Directory where data is prepped')
   parser.add_argument('-rdn', '--ready-dir-name', type=str,   default='/tmp',                                  help='Directorry where data is ready to be sent')
   args = parser.parse_args()

   failed = 0 
   prep_dir_name = validate_dirs(args.prep_dir_name) 
   if prep_dir_name is False: 
      print("Invalid prep dir (%s)" % prep_dir_name) 
      failed += 1 
   ready_dir_name = validate_dirs(args.ready_dir_name) 
   if ready_dir_name is False: 
      print("Invalid ready dir (%s)" % ready_dir_name) 
      failed += 1 
   if failed > 0: 
      exit(1) 

   latest_timestamp = get_last_timestamp(args.user_info, args.port, args.dbn, args.table)
   query = "SELECT * FROM %s WHERE time > '%s' AND time <= NOW();" % (args.table, latest_timestamp) 

   gd = GetData(args.url, args.url_db, query, prep_dir_name, ready_dir_name)
   gd.execute_process() 

if __name__ == '__main__': 
   main()
