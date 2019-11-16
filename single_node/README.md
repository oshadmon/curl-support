# Env
The following setup requires 1 machine which will act your Query, Publisher and Operator nodes. 


# Directions 
0. Update code to latest
```
cd $HOME/AnyLog-demo    # Source
git pull origin ${BRANCH_NAME} 

cd $HOME/pi-download/   # Scripts 
git pull origin ${BRANCH_NAME} 
``` 

1. Clean Env and Create Directory Structure Create Structure 
```
bash ~/AnyLog-demo/scripts/remove_dir_structure.sh ~/AnyLog-demo 
bash ~/AnyLog-demo/scripts/create_dir_structure.sh ~/AnyLog-demo 
psql -d postgres -c "DROP DATABASE IF EXISTS ${database_name}" 
```

2. Update Database and Node names in config_all.anylog 
```
cd ~/pi-download/anylog_connector/single_node
vim config_all.anylog 
``` 

3. Start sending data into AnyLog 
```
screen -S anylog -m bash -c "cd; python3 $HOME/AnyLog-demo/source/cmd/user_cmd.py process $HOME/pi-download/anylog_connector/single_node/single_node_demo.anylog" 
```  

## Example 
```
# Start process 
screen -S anylog -m bash -c "cd; python3 $HOME/AnyLog-demo/source/cmd/user_cmd.py process $HOME/pi-download/anylog_connector/single_node/single_node_demo.anylog"

# Execute queries 

# exit screen 
ctrl+a+d 

# re-enter 
screen -R anylog 
```

