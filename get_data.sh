for table in pressure temperature humidity 
do
python3  $HOME/curl-support/get_data.py anylog@192.168.1.236:demo 5432 pi_sensor_mx_south ${table} -pdn $HOME/AnyLog-Network/data/prep -rdn $HOME/AnyLog-Network/data/watch 
done
