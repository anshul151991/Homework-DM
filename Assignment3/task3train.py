from pyspark import SparkConf, SparkContext
import sys
import random
import time
import csv
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
i_file_loc = 'train_review.json'
#i_file_loc = sys.argv[1]
#o_file = sys.argv[4]
s_file = 'stopwords'
o_file = 'task3item.model'

cf_type = "user_based"

user_dict = dict()
bus_dict = dict()
user_dict_r = dict()
bus_dict_r = dict()


def sig_matr_create(line):
    hash_res = []

    for param in random_param:
        local_hash = list()
        a = param[0]
        b = param[1]
        for bus in line[1]:
            local_hash.append((((a * bus) + b) % 10259) % m)
        hash_res.append(min(local_hash))
    return (line[0], hash_res)

def lsh(line):
    ll = list()
    for i, hashs in enumerate(line[1],1):
        band = math.ceil(i / r)
        bucket = hashs % 3000
        ll.append((band, bucket))
    return ((line[0], ll))


def mergeDict(x,y):
    #print('x--> ',x)
    #print(type(x))
    #print('y --> ',y)
    #print(type(y))
    x.update(y)
    #print(x)
    return(x)

def pairGenerate(line):
    valid_pairs = list()
    list_pairs = list(itertools.combinations(sorted(line[1]), 2))
    #check jaccard similarity
    for pair in list_pairs:
        num = len(set(u_dict[pair[0]]).intersection(u_dict[pair[1]]))
        if num>2:
            sim = num/(len(set(u_dict[pair[0]]).union(u_dict[pair[1]])))
            if sim>0.01:
                valid_pairs.append(pair)
    return(valid_pairs)

def pearsonCorrelation(tup):
    u1 = user_dict_r[tup[0]]
    u2 = user_dict_r[tup[1]]
    co_rated_bus = set(ub_dict[u1].keys()).intersection(set(ub_dict[u2].keys()))
    len_co_rated_bus = len(co_rated_bus)
    num = 0
    den1 = 0
    den2 = 0
    avg_u0 = sum([ub_dict[u1][i] for i in co_rated_bus])/len_co_rated_bus
    avg_u1 = sum([ub_dict[u2][i] for i in co_rated_bus])/len_co_rated_bus
    for bus in co_rated_bus:
        num += (ub_dict[u1][bus] - avg_u0) * (ub_dict[u2][bus] - avg_u1)
        den1 += (ub_dict[u1][bus] - avg_u0) ** 2
        den2 += (ub_dict[u2][bus] - avg_u1) ** 2
    den1 = math.sqrt(den1)
    den2 = math.sqrt(den2)
    den = den1 * den2
    if den != 0:
        weight = num / den
    else:
        weight = 0
    return((u1,u2,weight))



#setting Spark session
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
r_file = sc.textFile(i_file_loc)

if cf_type =='item_based':
    # load input file
    #r_file = sc.textFile(i_file_loc)

    input_rdd = r_file.map(lambda x: (json.loads(x)["business_id"],{json.loads(x)["user_id"]:json.loads(x)["stars"]}))

    #input_rdd = r_file.map(lambda x: (json.loads(x)["business_id"],[(json.loads(x)["user_id"],json.loads(x)["stars"])]))
    #print('input rdd --> ',input_rdd.collect())
    grouped_input_rdd = input_rdd.reduceByKey(mergeDict)
    #print('grouped rdd --> ',grouped_input_rdd.collect())
    bus_usr_dict = grouped_input_rdd.collectAsMap()
    #print(bus_usr_dict)
    


    bus_singletons = bus_usr_dict.keys()

    pairs = [tuple(sorted(i)) for i in itertools.combinations(bus_singletons,2)]

    #print(type(pairs))
    #print(pairs)

    ll = list()
    for pair in pairs:
        #weight_dict = dict()
        #print('Processing for pair --> ',pair)
        if len(bus_usr_dict[pair[0]].keys())  < 3 or len(bus_usr_dict[pair[1]].keys()) < 3:
            #print('discarding pair --> ',pair)
            continue
        else:
            #print('in else')

            co_rated_usr = set(bus_usr_dict[pair[0]].keys()).intersection(set(bus_usr_dict[pair[1]].keys()))
            #print(co_rated_usr)

            len_co_usr = len(co_rated_usr)
            if len_co_usr < 3:
                #print('discarding pair due to length of co rated users less than 3 --> ', pair)
                continue
            num = 0
            den1 = 0
            den2 = 0
            avg_p0= sum([bus_usr_dict[pair[0]][i] for i in co_rated_usr])/len_co_usr
            avg_p1= sum([bus_usr_dict[pair[1]][i] for i in co_rated_usr]) / len_co_usr
            for usr in co_rated_usr:
                num +=  (bus_usr_dict[pair[0]][usr] - avg_p0) * (bus_usr_dict[pair[1]][usr] - avg_p1)
                den1 += (bus_usr_dict[pair[0]][usr] - avg_p0) ** 2
                den2 += (bus_usr_dict[pair[1]][usr] - avg_p1) **2
            den1 = math.sqrt(den1)
            den2 = math.sqrt(den2)
            den = den1*den2
            if den !=0:
                weight = num/den
            else:
                weight = 0
            #print('weight for pair ',pair,' is ', weight)
            if weight>0:
                #weight_dict["b1"] = pair[0]
                #weight_dict["b2"] = pair[1]
                #weight_dict["sim"] = weight
                ll.append([pair[0],pair[1],weight])
    
    with open(o_file, mode='w',newline='',encoding="utf-8") as out_file:

        for line in ll:
            out_file.write('{"b1": "' + line[0]+'", ')
            out_file.write('"b2": "' + line[1]+'", ')
            out_file.write('"sim": ' + str(line[2])+'}'+'\r\n')

elif cf_type =='user_based':
    input_rdd =r_file.map(lambda x:(json.loads(x)["user_id"],json.loads(x)["business_id"]))

    ub_rdd = r_file.map(lambda x: (json.loads(x)["user_id"],{json.loads(x)["business_id"]:json.loads(x)["stars"]}))
    gr_ub_rdd = ub_rdd.reduceByKey(mergeDict)
    ub_dict = gr_ub_rdd.collectAsMap()
    uid_list = input_rdd.map(lambda x:x[0]).distinct().collect()
    bid_list = input_rdd.map(lambda x: x[1]).distinct().collect()
    # of bin
    m = len(bid_list)
    random_param = [(random.randint(1, 1000), random.randint(1, 1000)) for i in range(0, 60)]

    for index, user in enumerate(uid_list):
        user_dict[user] = index
        user_dict_r[index] = user

    ptime = time.time()
    for index, bus in enumerate(bid_list):
        bus_dict[bus] = index
        bus_dict_r[index] = bus

    mod_uid_bid_rdd = input_rdd.map(lambda x: (user_dict[x[0]],bus_dict[x[1]])).groupByKey().map(lambda x:(x[0],list(x[1])))

    u_dict = mod_uid_bid_rdd.collectAsMap()
    sig_mtrx = mod_uid_bid_rdd.map(sig_matr_create)

    b=30
    r=2

    mod = sig_mtrx.map(lsh).flatMap(lambda x: [(i,x[0]) for i in x[1]]).groupByKey().map(lambda x: (x[0],set(x[1]))).filter(lambda x:len(x[1])>1).flatMap(pairGenerate).distinct()
    #cand_pairs = mod.collect()


    cand_pairs_rdd = mod.map(pearsonCorrelation)
    out_list = cand_pairs_rdd.collect()

    with open(o_file, mode='w', newline='', encoding="utf-8") as out_file:
        for line in out_list:
            out_file.write('{"u1": "' + line[0] + '", ')
            out_file.write('"u2": "' + line[1] + '", ')
            out_file.write('"sim": ' + str(line[2]) + '}' + '\r\n')

print('processing_time --> ',time.time() - stime)