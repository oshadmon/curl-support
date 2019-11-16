import datetime 
import glob
import os
import sys 
import time 

class FileIO: 
   def __init__(self, generate_data_prep:str, publisher_data_in:str): 
      self.generate_data_prep = generate_data_prep 
      self.publisher_data_in = publisher_data_in

   def __convert_file_size(self, file_size:float)->float: 
      """
      Convert file_size into MB
      :args: 
         file_size:float - file size 
      :return: 
         convert file_size into MB 
      """
      return file_size * 1000000


   def check_if_file_exists(self, sensor_id:str, name:str)->str: 
      """
      Based on sensor_id and sensor_name check if file exists 
      :args: 
         sensor_id:str - sensor sensor_id 
         sensor_name:str - name of the sensor 
      :return: 
         if exists get file else return False   
      """
      ret_value = ""
      file_name = '%s/%s.*.%s.json' % (self.generate_data_prep, sensor_id, name)
      try: 
         ret_value = glob.glob(file_name)
      except Exception as e:
         print("OSError - Failed top get file (%s) - %s" % (file_name, e))
         ret_value = False 
    
      if ret_value != False and len(ret_value) > 0: 
         ret_value = ret_value[0]

      return ret_value 

   def check_file_size(self, file_name:str)->bool: 
      """
      Get the size of a given file and check whether or not it has reached max size. 
      :args: 
         if reached max_size (self.file_size) return True 
         if did not reach max size return False 
         if there is an (OSError) when trying to get file size return -1 - this will exist program  
      """
      try:
         size = os.path.getsize(file_name)
      except Exception as e:
         print("Failed to get file file size for %s - %s" % (file_name, e))
         return -1

      if size >= self.file_size: 
         return True
      return False 

   def move_file(self, file_name:str, new_dir:str)->bool: 
      """
      Move file from generate_data_prep to publisher_data_in
      :args: 
         file_name:str - file that we would like to move 
         new_dir:str  - directory to move file to 
      :return: 
         if success return True, else False 
      """
      ret_value = True 
      new_dir = os.path.expanduser(os.path.expandvars(new_dir)) 
      try: 
         os.rename(file_name, new_dir+"/"+file_name.rsplit("/", 1)[-1]) 
      except: 
         print("Failed to move file (%s) to publisher (%s)" % (file_name, new_dir))
         ret_value = False 
      return ret_value 

   def create_file(self, sensor_id:str, timestamp:str, sensor_name:str)->str: 
      """
      Generate  file name and create  
      :args: 
         sensor_device:str - 
         sensor_id:str - sensor sensor_id 
         sensor_name:str - name of the sensor 
      :return: 
         if able to create file return name, else return False 
      """
      file_name = '%s/%s.%s.%s.json' % (self.generate_data_prep, sensor_id, timestamp, sensor_name)
      try: 
         open(file_name, 'w').close()
      except Exception as e: 
         print("Unable to create file (%s) - %s" % (self.generate_data_prep, e))
         return False 
      return file_name 

   def write_to_file(self, file_name:str, data:str)->bool:
      """
      Write to file 
      :args: 
         file_name:str - file to store into 
         data:str - string to write to file 
      :return: 
         if success return True, else return False 
      """
      ret_value = True 
      try: 
         with open(file_name, 'a') as f: 
            try: 
               f.write(data+'\n')
            except Exception as e: 
               print("Failed to append to file (%s) - %s" % (file_name, e))
               ret_value = False 
      except Exception as e: 
         print("failed to open to file (%s) - %s" % (file_name, e)) 
         ret_value = False 
      return ret_value 

   def read_from_file(self, file_name:str)->str: 
      """
      get data from file 
      :args: 
         file_name:str - file to read from
      :param: 
         ret_value:str - data to return 
      :return: 
         results read, if fail (at any point) return False 
      """
      ret_value = ""
      try: 
         with open(file_name, 'r') as f: 
            try: 
               ret_value = f.readlines()
            except Exception as e: 
               print("Unable to read data from file (%s) - %s" % (file_name, e))
               ret_value = False 
      except Exception as e: 
         print("Failed to open file (%s) - %s" % (file_name, e))
         ret_value = False 
      return ret_value  
