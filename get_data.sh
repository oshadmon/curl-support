for table in pressure temperature humidity 
do
python3 get_query.py http://trunoz.com:8086/query PI_SENSOR "SELECT * from ${table} where time >= '%s' and time <= '%s'" -i 1  -st 2019-10-27
done
