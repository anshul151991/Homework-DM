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
#o_file = sys.argv[4]
s_file = 'stopwords'
o_file = 'task2.model'

user_dict = dict()
bus_dict = dict()

#setting stopword list
stopwords_punc = ["(", "[", ",", ".", "!", "?", ":", ";", "]", ")"]
with open(s_file) as stop_f:
    data = stop_f.read()
    [stopwords_punc.append(word.strip()) for word in data.split()]
#print(stopwords_punc)


#setting Spark session
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

def remove_stopword(line):
    #print(line[1])
    ll = list()
    for word in line[2]:
        #print(word)
        if word not in stopwords_punc:
            #line[1].remove(word)
            #print(word)
            ll.append(word)
    #print(ll)
    return(line[0],line[1],ll)

def freq_compute(line):
    #print('line --> ',line)
    max = 0
    loc_freq_dict = dict()
    for word in line[1]:
        loc_freq_dict[word] = loc_freq_dict.get(word,0)+1
        if loc_freq_dict[word] > max:
            max = loc_freq_dict[word]
    #print('loc_freq_dict -->',loc_freq_dict)
    #print('max --> ',max)
    #Finding TF*IDF
    tf_idf_dict = dict()
    for k,v in loc_freq_dict.items():
        #print('Calculating for word - ',k)
        tf = v/max
        #print('tf = ',tf)
        idf = math.log(n_doc/inv_dict_counts[k])
        #print('idf = ', idf)
        prod = tf*idf
        #print('prod = ', prod)
        tf_idf_dict[k] = prod
    #print('tf_idf_dict --> ',tf_idf_dict)
    sorted_tf_idf_dict_list = sorted(tf_idf_dict.items(), key=lambda x:x[1], reverse=True)
    #print('sorted_tf_idf_dict --> ',sorted_tf_idf_dict_list)
    #print('first 1 value of sorted_tf_idf_dict  --> ', sorted_tf_idf_dict_list[:200])
    ll = [i[0] for i in sorted_tf_idf_dict_list[:200]]
    return(line[0],ll)


# load input file
r_file = sc.textFile(i_file_loc)

input_rdd = r_file.map(lambda x:(json.loads(x)["user_id"],json.loads(x)["business_id"], json.loads(x)["text"].lower()
                                 .replace("(",'').replace("[",'').replace(",",'').replace(".",'')
                                 .replace("!",'').replace("?",'').replace(":",'').replace(";",'')
                                 .replace("]",'').replace(")",'').split())).map(remove_stopword)
#print('input_rdd --> ',input_rdd.collect())


#grouped_input = input_rdd.groupByKey().map(lambda x:(x[0],list(x[1])))
grouped_input = input_rdd.map(lambda x:(x[1],x[2])).reduceByKey(lambda a,b: a+b)
#print('gr input --> ',grouped_input.collect())
n_doc = grouped_input.count()
print('total number of documents --> ',n_doc)
#remove punctuations and stop words
'''filtered_input = grouped_input.map(remove_stopword).persist()
print('filtered input --> ',filtered_input.collect())
#sys.exit(1)
'''
#for inverse doc frequency
inv_dict_counts = dict()

inv_dict_counts = grouped_input.map(lambda x: set(x[1])).flatMap(lambda x: [(i,1) for i in x]).reduceByKey(lambda a,b: a+b).collectAsMap()
#print('inv_dict_counts --> ',inv_dict_counts)
#sys.exit(1)

#finding TF.IDF
bus_prof = grouped_input.map(freq_compute)
#print(bus_prof.collect())
bus_prof_dict = bus_prof.collectAsMap()
print('BUSINESS PROFILE DICT --> ',bus_prof_dict)
#sys.exit(1)
#Creating User Profile

user_prof = input_rdd.map(lambda x:(x[0],[x[1]])).reduceByKey(lambda a,b: a+b)
print('Number of users --> ',user_prof.count())
user_prof_dict = user_prof.collectAsMap()
print('hi')
#print('user_prof_list --> ',user_prof_dict)
user_prof_dict2 = dict()
for k,v in user_prof_dict.items():
    user_prof_dict2[k] = list()
    for i in v:
        user_prof_dict2[k].extend(bus_prof_dict[i][:10])
    #[user_prof_dict2[k].append(bus_prof_dict[i][:10]) for i in v]
#print('user_prof_dict2 --> ',user_prof_dict2)
#out_dict['business'] = bus_prof_dict
#out_dict['review'] = user_prof_dict2



with open(o_file, mode='w',newline='',encoding="utf-8") as out_file:
    #employee_writer = csv.writer(out_file, delimiter='|',quoting=csv.QUOTE_NONE,escapechar='\\')
    #employee_writer.writerow(['type', 'id', 'list_words'])

    for k,v in bus_prof_dict.items():
        out_file.write("business\x01"+k+'\x01'+str(v)+'\r\n')
    for k,v in user_prof_dict2.items():
        #print('here')
        #employee_writer.writerow(['review', k, v])
        out_file.write("user\x01" + k + '\x01' + str(v) + '\r\n')

'''
with open(o_file, mode='w',newline='') as out_file:
    employee_writer = csv.writer(out_file, delimiter='|',quoting=csv.QUOTE_NONE,escapechar='\\')
    employee_writer.writerow(['type', 'id', 'list_words'])
    for k,v in bus_prof_dict.items():
        employee_writer.writerow(['business', k, v])
    for k,v in user_prof_dict2.items():
        employee_writer.writerow(['review', k, v])
'''
print('Process end time --> ',time.time() - stime)