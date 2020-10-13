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
i_file_loc = 'task2.model'
#o_file = sys.argv[4]
t_file_loc = 'test_review.json'
o_file = 'task2.predict'

user_dict = dict()
bus_dict = dict()


def compute_sim(line):
    user_words = set(user_dict.get(line[0],''))
    bus_words = set(bus_dict.get(line[1],''))
    num = len(user_words.intersection(bus_words))
    den = math.sqrt(len(user_words))*math.sqrt(len(bus_words))
    if den ==0:
        sim = 0
    else:
        sim = num/den
    return('{"user_id": "'+line[0] + '", "business_id": "' + line[0] + '", "sim": ' + str(sim) + '}')


#setting Spark session
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


# load input file
r_file = sc.textFile(i_file_loc)
#print(r_file.collect())


#create user dict and bus dict
a = r_file.map(lambda x:x.split('\x01')).persist()
#print(a.collect())
#sys.exit(1)
#bus_dictt = a.filter(lambda x: x[0] == 'business').map(lambda x:(x[1],x[2].strip()))
#print('a -->',bus_dictt.collect())
bus_dict = a.filter(lambda x: x[0] == 'business').map(lambda x:(x[1],eval(x[2]))).collectAsMap()
user_dict = a.filter(lambda x: x[0] == 'user').map(lambda x:(x[1],eval(x[2]))).collectAsMap()



#b = r_file.map(lambda x:for i in x.split('|'))
#print(b.collect())

t_file = sc.textFile(t_file_loc)
t_file_json = t_file.map(lambda x: (json.loads(x)["user_id"],json.loads(x)["business_id"]))

out_tup = t_file_json.map(compute_sim).filter(lambda x:eval(x)["sim"] > 0.01)
out_str = out_tup.collect()
#print('out_str --> ',out_str)

with open(o_file,'w',newline='') as out_file:
    #out_writer = csv.writer(out_file,delimiter='|',quoting=csv.QUOTE_MINIMAL)
    #out_writer.writerow(['user_id','business_id'])
    for line in out_str:
        out_file.write(line+'\n')

print('Processing time  : ',time.time() - stime)