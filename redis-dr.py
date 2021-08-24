#!/bin/env python
#coding:utf8

"""
redis dump and restore
work if redis server has [dump] and [restore] command (Available since 2.6.0)
dump file not suppose to readable
"""
import re
import time
import redis
from traceback import print_exc


line_split_flag="   ///   "     #key's name should not have substring like line_split_flag+".*?"+line_split_flag
absolute_time_tag="#"           #tag for absolute time
default_dump_file="./redis.dump"


def redis_fuzzy_search(redis_client, match, count=100, match_len=10, max_try=10):
    """
    使用scan迭代查询
    redis_client   
    match          
    count          
    match_len     匹配的长度超过这个即结束，0则为遍历所有 
    max_try       最多的尝试次数，0为不限制直至遍历所有 
    """
    i = 0
    result = []
    cursor = 0
    while True:
        cursor, r = redis_client.scan(cursor, match, count)
        result += r
        i += 1
        
        if not match_len and len(result):
            yield result
            result = []
        
        # 取到值并满足长度要求
        if match_len and len(result)>=match_len: 
            yield result
            result = []
            break
        
        # 遍历结束或者达到遍历上限
        if (not cursor) or (max_try and i>=max_try):
            if result:
                yield result
            break


def get_redis_client(host="127.0.0.1",port=6379,db=0,password=None):
    print(host,port,db,password)
    #latin1 单字节编码，不会丢失数据
    return redis.StrictRedis(host=host,port=port,db=db,password=password,decode_responses=True,encoding="latin1")
    #return redis.StrictRedis(host=host,port=port,db=db,password=password)


def _dumps(redis_client,absolute,k):
    v=redis_client.dump(k)
    t=redis_client.ttl(k)
    if t==-1:
        t=0
    if absolute:
        t=absolute_time_tag+str(time.time()+t)
    if v==None:
        return None
    else:
        k=k.replace("\n","\\\\r")
        v=v.replace("\n","\\\\r")      #dump string may contain '\n',convert it
        l="%s%s%s%s%s\n" % (k,line_split_flag,t,line_split_flag,v)
        return l
                
                
def dumps(redis_client,absolute,scan=True):
    i=0
    if scan:
        print("use scan")
        for keys in redis_fuzzy_search(redis_client,"*",100,0,0):
            for k in keys:
                dump_str = _dumps(redis_client,absolute,k)
                if dump_str:
                    i += 1
                    yield dump_str
    else:
        #有些比较大，keys会比较危险
        for k in redis_client.keys():
            dump_str = _dumps(redis_client,absolute,k)
            if dump_str:
                i += 1
                yield dump_str
    
    print("dump keys "+str(i))
        
        
def dump(f="/tmp/redis.dump",redis_client=None,absolute=False,scan=True):       

    fw=open(f,"wb")
    fw.close()             #clear old file

    counter = 0
    for l in dumps(redis_client,absolute,scan):
        if not counter:
            fw=open(f,"ab")
       
        #unicode -> byte
        fw.write(l.encode('latin1'))
        counter = (counter + 1) % 1000
        if not counter:
            fw.close()
    if counter:
        fw.close()    


def restores(f):
    f=open(f,"rb")
    l=f.readline()
    while l:
        #byte -> unicode
        l=l.decode('latin1')
        yield l
        l=f.readline()        

    f.close()


def restore(f,redis_client=None,replace=False,force=False):
    counter = 0
    i = 0
    for l in restores(f):
        i += 1
        try:
            new_split_pattern="%s.*?%s" % (line_split_flag,line_split_flag)
            new_split_str=re.findall(new_split_pattern,l)[0]
           
            k=l.split(new_split_str)[0]
            t=new_split_str.strip(line_split_flag)
            #print([k,t])
            v=l.lstrip("%s%s%s%s" % (k,line_split_flag,t,line_split_flag)).rstrip("\n")

            if re.search(absolute_time_tag+".*",t):
                t=t.lstrip(absolute_time_tag)
                t=float(t)-time.time()

            t=int(t)
            if t<0:
                t=0
            t=t*1000
       
            v=v.replace("\\\\r","\n")                           #reconvert 
            k=k.replace("\\\\r","\n")
            if not counter:
                p = redis_client.pipeline(transaction=False)              
            #print([k,t,v])
            p.restore(k,t,v,replace)
            counter = (counter + 1) % 1000
            if not counter:
                p.execute()
            
        except:
            if not force:
                print_exc()
                raise Exception("error in [%s,%s,%s]" % (k,t,v))
    
    if counter:
        try:
            p.execute()
        except:
            if not force:
                print_exc()
                raise Exception("error in last execute")
    
    print("restore keys "+str(i))

 
def main(parser):

    options, args = parser.parse_args()
    
    if len(args)!=1:
        parser.print_help()
        exit(4)
    
    action=args[0]

    def options_to_kwargs(options):
        args={}
        if options.host:
            args["host"] = options.host
        if options.port:
            args["port"] = int(options.port)
        if options.password:
            args["password"] = options.password
        if options.db:
            args["db"] = int(options.db)
        return args
    
    kwargs=options_to_kwargs(options)
    
    f=options.file or default_dump_file
    print(f)

    absolute = options.absolute in ["True","true"]
    replace = options.replace in ["True","true"]
    scan = True
    if options.scan:
        scan = options.scan in ["True","true"]
    
    force = options.force in ["True","true"]
    
    if action=="dump":
        r=get_redis_client(**kwargs)
        dump(f,r,absolute,scan)

    elif action=="restore":
        r1=get_redis_client(**kwargs)
        restore(f,r1,replace,force)

    else:
        parser.print_help()
        

if __name__=="__main__":
    import optparse
    usage = "Usage: %prog dump|restore [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-H", "--host", help="Server hostname (default: 127.0.0.1).")
    parser.add_option("-p", "--port", help="Server port (default: 6379).") 
    parser.add_option("-n", "--db",   help="Database number. (default: 0)")
    parser.add_option("-a", "--password", help="Password to use when connecting to the server.(default: None)")
    parser.add_option("-f", "--file",     help="File for output or input.(default: ./redis.dump)")    
    parser.add_option("-b", "--absolute", help="Only in dump, Dump key use absolute time for expire.(default: false. true|false)")
    parser.add_option("-s", "--scan",     help="Only in dump, use scan replace keys.(default: true. true|false)")
    parser.add_option("-r", "--replace",  help="Only in restore, replace key if exist.(default: false. true|false)")
    parser.add_option("-F", "--force",    help="Only in restore, skip error when.(default: false. true|false)")

    main(parser)

    
