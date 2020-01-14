import redis
import json

r = redis.Redis("localhost")
p = r.pipeline()
counter = 1

with open("/Users/chadbean/Documents/Programing/github/email\ validation/load.json") as f:
    data = f.readlines()
    
    for record in data:
        jrecord = json.loads(record)
        for key, value in jrecord.items():
            try:
                if int(value) > 1:
                    p.zincrby(jrecord['domain'], key, value)
                  
            # I wasnt sure a better way to skip values that couldnt be turned into an int from above code so I 
            # just caught the error and skip        
            except ValueError:
                continue
    p.execute()
