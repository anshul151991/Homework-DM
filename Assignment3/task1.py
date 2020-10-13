from pyspark import SparkConf, SparkContext
import sys
import random
import time
import json
import math
import itertools
from pyspark.sql import SparkSession

# print('System arguments --> ',sys.argv)
# Initializing input variables
stime = time.time()
out_dict = dict()
delm = ','
#ct_dict = {}
#case = int(sys.argv[1])
#support = int(sys.argv[2])
i_file_loc = sys.argv[1]
o_file = sys.argv[2]


user_dict = dict()
bus_dict = dict()
user_dict_r = dict()
bus_dict_r = dict()

def signature_matrix(line):
    #hash function - (ax+b)%m
    hash_res = []

    for param in random_param:
        local_hash = list()
        a = param[0]
        b = param[1]
        for usr in line[1]:
            local_hash.append((((a*usr) + b) % 26189) % m)
        hash_res.append(min(local_hash))
    return(line[0],hash_res)

def lsh_bucket(line):
    #hash_function = a % 5000
    ll = list()
    for i,hash in enumerate(line[1]):
        band = math.ceil(i/r)
        bucket = hash % 5000
        ll.append((band,bucket))
    return((line[0],ll))


spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# load input file
r_file = sc.textFile(i_file_loc)

input_rdd = r_file.map(lambda x:(json.loads(x)["user_id"],json.loads(x)["business_id"])).persist()

#uid_bid_rdd = input_rdd.groupByKey().map(lambda x: (x[0],list(x[1])))
#print(uid_bid_rdd.take(5))
utime = time.time()
#distinct users
uid_rdd = input_rdd.map(lambda x:x[0]).distinct().collect() #26184
#print(uid_rdd)
# of bins
m = len(uid_rdd)
random_param = [(random.randint(1,1000),random.randint(1,1000)) for i in range(0,50)]
#print(len(random_param))
#distinct businesses
bid_rdd = input_rdd.map(lambda x:x[1]).distinct().collect() #10253


#print(bid_rdd)
utime = time.time()
for index,user in enumerate(uid_rdd):
    user_dict[user] = index
    user_dict_r[index] = user

ptime = time.time()
for index,bus in enumerate(bid_rdd):
    bus_dict[bus] = index
    bus_dict_r[index] = bus

#mod_uid_bid_rdd = input_rdd.map(lambda x: (user_dict[x[0]], bus_dict[x[1]])).groupByKey().map(lambda x:(x[0],list(x[1])))
mod_bid_uid_dict = input_rdd.map(lambda x: (bus_dict[x[1]], user_dict[x[0]])).groupByKey().map(lambda x:(x[0],list(x[1])))
#print('### ', mod_bid_uid_dict.collect())

b_dict = mod_bid_uid_dict.collectAsMap()
mtime = time.time()
mtrx = mod_bid_uid_dict.map(signature_matrix)
aaaa = mtrx.collect()
print('########### signature matrix - ',time.time()-mtime)
#print(mtrx.collect())
b = 25
r = 2

mod = mtrx.map(lsh_bucket).flatMap(lambda x: [(i,x[0]) for i in x[1]]).groupByKey().map(lambda x: (x[0],set(x[1]))).filter(lambda x:len(x[1])>1)
#print(mod.collect())
print(mod.count())

print('########### lsh - ',time.time()-stime)
cand_set = set()
for i in mod.collect():
     [cand_set.add(j) for j in list(itertools.combinations(sorted(i[1]), 2))]
#print('cand_set --> ',cand_set)

print('########### compute pair - ',time.time()-stime)
out_dict = dict()
#print(b)
for pair in cand_set:
    bus1 = pair[0]
    bus2 = pair[1]
    s_bus1 = set(b_dict[bus1])
    s_bus2 = set(b_dict[bus2])
    num = len(s_bus1.intersection(s_bus2))
    den = len(s_bus1.union(s_bus2))
    sim = num/den
    if sim > 0.05:
        out_dict[pair] = sim
#print(out_dict)
print('########### compute sim - ',time.time()-stime)
with open(o_file, mode='w',newline='',encoding="utf-8") as out_file:

    for k,v in out_dict.items():
        out_file.write('{"b1": "' + bus_dict_r[k[0]]+'", ')
        out_file.write('"b2": "' + bus_dict_r[k[1]]+'", ')
        out_file.write('"sim": ' + str(v)+'}'+'\r\n')

print('Processing time --> ',time.time()-stime)



