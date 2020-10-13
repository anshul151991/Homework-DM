from pyspark import SparkConf, SparkContext
import sys
import random
import time
import json
import math
import csv
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
train_file_loc = 'train_review_small.json'
#o_file = sys.argv[4]


t_file_loc = 'test_review_small1.json'
o_file = 'task3usersmall.predict'
m_file = 'task3_small.model'
cf_type = "user_based"
n=15
user_dict = dict()
bus_dict = dict()

def compute_sim(line):
    usr_id = line[0]
    bus_id = line[1]
    #print('## ',usr_id)
    #print('## ', bus_id)
    busid_list = set(usr_bus_dict[usr_id].keys())
    #generating pairs
    pairs = [tuple(sorted((i,bus_id))) for i in busid_list]
    #checking the weight of each pair and selecting only the top N pairs
    pairs_sim = [(i,m_dict.get(i,0)) for i in pairs]
    pairs_sim.sort(key = lambda x:x[1],reverse=True)
    num = 0
    den =0
    for i in pairs_sim[:n]:
        bid=i[0][1] if i[0][1] != bus_id else i[0][0]
        num += usr_bus_dict[usr_id][bid]*i[1]
        den += i[1]
    if den != 0:
        p_rating = num/den
    else:
        p_rating = 0
    return(usr_id,bus_id,p_rating)

def usr_based_pred(line):
    usr = line[0]
    bus = line[1]
    
    avg_usr = sum(usr_bus_dict[usr].values())/len(usr_bus_dict[usr].values())
    
    if bus_usr_dict.get(bus) is None:
        return(usr, bus, None)
    usr_list = bus_usr_dict[bus].keys()
    pairs = [tuple(sorted((i,usr))) for i in usr_list]
    pairs_sim = [(i, m_dict.get(i, 0)) for i in pairs]
    pairs_sim.sort(key=lambda x: x[1], reverse=True)
    num = 0
    den = 0
    
    for i in pairs_sim[:n]:

        uid = i[0][1] if i[0][1] != usr else i[0][0]
        
        #compute avg for uid
        co_rated_bus = set(usr_bus_dict[usr].keys()).intersection(usr_bus_dict[uid].keys())
        if len(co_rated_bus) ==0:
            avg_uid = 0
        else:
            avg_uid = sum([usr_bus_dict[uid][i] for i in co_rated_bus])/len(co_rated_bus)
        #print('rating -->',usr_bus_dict[uid][bus])
        #print('avg -->',avg_uid)
        #print('weight -->',i[1])
        num += (usr_bus_dict[uid][bus]-avg_uid) * i[1]
        den += math.fabs(i[1])
    

    if den != 0:
        p_rating = avg_usr + (num / den)
    else:
        p_rating = avg_usr
    return (usr, bus, p_rating)



def mergeDict(x, y):
    # print('x--> ',x)
    # print(type(x))
    # print('y --> ',y)
    # print(type(y))
    x.update(y)
    # print(x)
    return (x)
#setting Spark session
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# load input file
r_file = sc.textFile(train_file_loc)
if cf_type == 'item_based':

    #print(r_file.collect())
    input_rdd = r_file.map(lambda x: (json.loads(x)["user_id"],{json.loads(x)["business_id"]:json.loads(x)["stars"]}))
    grouped_input_rdd = input_rdd.reduceByKey(mergeDict)
    usr_bus_dict = grouped_input_rdd.collectAsMap()



    t_file = sc.textFile(t_file_loc)
    t_file_json = t_file.map(lambda x: (json.loads(x)["user_id"],json.loads(x)["business_id"]))

    model_file = sc.textFile(m_file)
    m_dict = model_file.map(lambda x: (tuple(sorted((json.loads(x)["b1"],json.loads(x)["b2"]))),json.loads(x)["sim"])).collectAsMap()

    out_tup = t_file_json.map(compute_sim)#.filter(lambda x:eval(x)["sim"] > 0.01)

    out_list = out_tup.collect()


    with open(o_file,'w',newline='') as out_file:
        #out_writer = csv.writer(out_file,delimiter='|',quoting=csv.QUOTE_MINIMAL)
        #out_writer.writerow(['user_id','business_id'])
        for line in out_list:
            out_file.write('{"user_id": "' + line[0] + '", ')
            out_file.write('"business_id": "' + line[1] + '", ')
            out_file.write('"stars": ' + str(line[2]) + '}' + '\r\n')

elif cf_type == 'user_based':
    input_rdd = r_file.map(lambda x: (json.loads(x)["business_id"], {json.loads(x)["user_id"]: json.loads(x)["stars"]}))
    grouped_input_rdd = input_rdd.reduceByKey(mergeDict)
    bus_usr_dict = grouped_input_rdd.collectAsMap()
    #print(r_file.collect())
    usr_bus_rdd = r_file.map(lambda x: (json.loads(x)["user_id"],{json.loads(x)["business_id"]:json.loads(x)["stars"]}))
    #print(usr_bus_rdd.collect())
    grouped_usr_bus_rdd = usr_bus_rdd.reduceByKey(mergeDict)
    usr_bus_dict = grouped_usr_bus_rdd.collectAsMap()
    #print(usr_bus_dict)



    model_file = sc.textFile(m_file)
    m_dict = model_file.map(lambda x: (tuple(sorted((json.loads(x)["u1"], json.loads(x)["u2"]))), json.loads(x)["sim"])).collectAsMap()

    t_file = sc.textFile(t_file_loc)
    t_file_json = t_file.map(lambda x: (json.loads(x)["user_id"], json.loads(x)["business_id"]))
    out_tup = t_file_json.map(usr_based_pred)  # .filter(lambda x:eval(x)["sim"] > 0.01)
    out_list = out_tup.collect()

    with open(o_file,'w',newline='') as out_file:
        #out_writer = csv.writer(out_file,delimiter='|',quoting=csv.QUOTE_MINIMAL)
        #out_writer.writerow(['user_id','business_id'])
        for line in out_list:
            out_file.write('{"user_id": "' + line[0] + '", ')
            out_file.write('"business_id": "' + line[1] + '", ')
            out_file.write('"stars": ' + str(line[2]) + '}' + '\r\n')

print('Processing time  : ',time.time() - stime)