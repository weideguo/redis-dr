# redis-dr

[![Python 2.7|3.7](https://img.shields.io/badge/python-2.7%7C3.7-blue.svg)]() 

redis dump and restore  


example
--------------
help  
./redis-dr.py


dump  
./redis-dr.py dump -p 6379 -n 6 -f redis.dump 

restore  
./redis-dr.py restore -p 6379 -n 9 -f redis.dump -r true


