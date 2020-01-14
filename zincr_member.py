import sys
import csv
import redis

r = redis.Redis("10.138.0.5")
p = r.pipeline()
counter = 1

with open(sys.argv[1]) as f:
    csvr = csv.reader(f)
    for row in csvr:
        row[0] = ''.join(row[0]).split()
        title = row[0]
        count = row[-1]
        try:
            count = int(count)
        except:
            continue
        for i in row[0]:
            p.zincrby("titles_split_2", count, i)
        counter += 1
#        if count % 1000:
        if counter == 300000: 
           print('executed')
           print(count)
           p.execute()
           counter = 1

p.execute()
