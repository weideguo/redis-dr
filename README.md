redis dump and restore tool

# example
./redis-dr.py


dump
./redis-dr.py dump -p 6401 -n 6 -f redis.dump 

restore
./redis-dr.py restore -p 6401 -n 9 -f redis.dump -r true


