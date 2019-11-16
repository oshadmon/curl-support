import argparse
import datetime 
import os
import requests
import time 

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

class GetData: 
   def __init__(self, url:str, db:str, query:str, iterations:int, sleep:float, prep_dir_name:str, ready_dir_name:str):
      """
      Get data from URL and store into files in query 
      :param: 
         self.url:str - URL to get data from 
         self.db:str - logical database to get data from 
         self.query:str - Query to get data 
         self.iterations:int - Number of times to get data 
         self.sleep:float - Number of seconds to sleep between each iteration
         self.prep_dir_name:str - directory where data is prepped 
         self.ready_dir_name:str - directory where data is ready to be sent 
      """
      self.url = url 
      self.db  = db 
      self.query = query 
      self.iterations = iterations 
      self.sleep = sleep 
      self.prep_dir_name = prep_dir_name 
      self.ready_dir_name = ready_dir_name 

   def request_data(self)->dict: 
      """
      execute cURL command to get data 
      :return: 
         result based on query
      """
      params = {'db': self.db, 'q': self.query} 
      try: 
         return requests.get(self.url, params=params).json()
      except:  
         return False 

   def format_data(self, request_result:dict)->(str, list): 
      """
      Given results, get relevent information
      :args: 
         request_result:str - raw results
      :param: 
         data_set:list - list of formatted results 
         table_name:str - table to store data into (based on  data in JSON) 
         table_columns:str - list of columns 
      :return: 
         table name and formatted data set 
      """
      data_set = [] 
      for key in list(request_result.keys()): 
         for row in request_result[key]: 
            table_name = row['series'][0]['name'] 
            table_columns = row['series'][0]['columns']
            for value in row['series'][0]['values']: 
               data = {} 
               for column in table_columns: 
                  data[column] = value[table_columns.index(column)]
               data_set.append(data) 
      return table_name, data_set

   def get_data_main(self): 
      request_result = self.request_data() 
      table_name, self.format_data(request_result))

def main(): 
   parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
   parser.add_argument('url',                      type=str,   default=None,                                    help='URL to get data from') 
   parser.add_argument('db',                       type=str,   default=None,                                    help='Lgical database to get data from') 
   parser.add_argument('query',                    type=str,   default=None,                                    help='Query to get data') 
   parser.add_argument('-i',   '--iterations',     type=int,   default=0,                                       help='Number of times to get data') 
   parser.add_argument('-s',   '--sleep',          type=float, default=10,                                      help='Number of seconds between each iteration')
   parser.add_argument('-pdn', '--prep-dir-name',  type=str,   default='$HOME/AnyLog-demo/data/publisher/prep', help='Directory where data is prepped')
   parser.add_argument('-rdn', '--ready-dir-name', type=str,   default='$HOME/AnyLog-demo/data/publisher/in',   help='Directorry where data is ready to be sent')
   args = parser.parse_args()

   prep_dir_name = validate_dirs(args.prep_dir_name) 
   ready_dir_name = validate_dirs(args.ready_dir_name) 

   if prep_dir_name != False and ready_dir_name != False: 
      gd = GetData(args.url, args.db, args.query, args.iterations, args.sleep, prep_dir_name, ready_dir_name)
      gd.get_data_main() 
   else: 
      print("Failed to start process") 

if __name__ == '__main__': 
   main()
