import json
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import time


#Initializing input variables
stime = time.time()
out_dict = dict()
i_file = sys.argv[1]
o_file = sys.argv[2]
s_file = sys.argv[3]
year = int(sys.argv[4])
top_usr = int(sys.argv[5])
stopword_f = sys.argv[5]
n_freq = int(sys.argv[6])




#setting stopword list
stopwords_punc = ["(", "[", ",", ".", "!", "?", ":", ";", "]", ")"]
with open(s_file) as stop_f:
    data = stop_f.read()
    [stopwords_punc.append(word.strip()) for word in data.split()]
#print(stopwords_punc)



#Setting spark Session Initial Code

#conf = SparkConf().setAppName("task1").setMaster("local").set("spark.driver.memory", "4g")
#sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

#load review file
r_file = sc.textFile(i_file).persist()


#solution 1.A - Total Number of reviews
review_ct = r_file.count()
#print('count',review_ct)

out_dict["A"] = review_ct


#solution 1.B - Total Number of reviews in a given year
rvs_given_year = r_file.filter(lambda line: datetime.strptime(json.loads(line)['date'],"%Y-%m-%d %H:%M:%S").year == year)
n_rvs = rvs_given_year.count()
out_dict["B"] = n_rvs


#solution 1.C - Number of distinct users
usr_rdd = r_file.map(lambda line : (json.loads(line)['user_id'].strip(),1)).groupByKey().persist()
distinct_usr_ct = usr_rdd.count()
#print(distinct_usr_ct)
out_dict["C"] = distinct_usr_ct


#solution 1.D - n Users with largest number of reviews and their count
user_wt_rv_ct = usr_rdd.map(lambda x:[x[0],len(list(x[1]))]).sortBy(lambda x:(-x[1],x[0])).take(top_usr)
#print (user_wt_rv_ct)
out_dict["D"] = user_wt_rv_ct


#solution 1.E - Top n frequent words
text_rdd = r_file.flatMap(lambda line: [(i,1) for i in json.loads(line)['text'].lower().split() if i not in stopwords_punc])

#map_rdd = text_rdd.map(lambda x: [(i,1) for i in x if i not in stopwords_punc]).groupByKey()
#print('text rdd',text_rdd.collect())
#print('map rdd',map_rdd.collect())
#text_rdd = r_file.flatMap(lambda line: (text,1) for text in json.loads()['text'].lower().replace("(",'').replace("[",'').replace(",",'').replace(".",'').replace("!",'').replace("?",'').replace(":",'')
#                     .replace(";",'').replace("]",'').replace(")",'').split() if text not in stopwords_punc).groupByKey()
    #.flatMap(lambda x:x.split())\
    #.map(lambda x: (x,1))\
    #.filter(lambda x: x[0] not in stopwords_punc)\
    #.groupByKey()
#print('text rdd',text_rdd.collect())

text_ct_rdd = text_rdd.groupByKey().map(lambda x: (x[0],len(x[1])))
#print('text ct rdd',text_ct_rdd.collect())
top_text_ct = text_ct_rdd.sortBy(lambda x:(-x[1],x[0])).take(n_freq)
#print('final',top_text_ct)
out_dict["E"] = top_text_ct

#print(top_word)


with open(o_file,'w') as out_file:
    json.dump(out_dict,out_file)


print('time -->',time.time()-stime)