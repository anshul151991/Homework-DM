import json
import sys
import time
from pyspark import SparkConf, SparkContext

#setting Input Variables

stime = time.time()
r_file = sys.argv[1]
b_file = sys.argv[2]
o_file = sys.argv[3]
process_ind = sys.argv[4]
n_top = int(sys.argv[5])
out_dict = dict()

review = dict()
business = dict()
joined_dict = dict()
rating_list = list()
avg_dict = dict()
avg_list = list()

#If process indicator is for spark
if process_ind.lower() == 'spark':
    #setting spark context
    conf = SparkConf().setAppName("task1").setMaster("local").set("spark.driver.memory", "4g")
    sc = SparkContext(conf=conf)

    #Creating RDD from input files
    revRdd = sc.textFile(r_file)
    busRdd = sc.textFile(b_file)

    # creating custom business Rdd
    cus_bus_rdd = busRdd.map(lambda line : (json.loads(line)['business_id'].strip(),json.loads(line)['categories']) if json.loads(line)["categories"] is not None else 'null').filter(lambda x: x != "null").flatMap(lambda x: [(x[0],y.strip()) for y in x[1].split(',')])
    #print('business file',cus_bus_rdd.collect())

    # creating custom review Rdd
    cus_rev_rdd = revRdd.map(lambda line : (json.loads(line)['business_id'],json.loads(line)['stars']))
    #print('review file',cus_rev_rdd.collect())

    # joining review and business Rdd
    joinedRdd = cus_bus_rdd.join(cus_rev_rdd).map(lambda x: x[1])
    #print('joined Rdd',joinedRdd.collect())

    # grouping joined RDD by key
    grpRdd = joinedRdd.groupByKey()
    #print('grpRdd',grpRdd.collect())

    # computing average of every category
    avgRdd = grpRdd.map(lambda x: [x[0], sum(x[1])/len(x[1]) ])
    #print('avgRdd ',avgRdd.collect())

    # sorting the Rdd and taking the top n categories
    sortedRdd = avgRdd.sortBy(lambda x:(-x[1],x[0])).take(n_top)
    #print('sortedRdd ',sortedRdd)

    out_dict['result'] = sortedRdd
    #print(out_dict)

#If process indicator is for python
elif process_ind.lower() == 'no_spark':
    #print('inside python')
    #reading review file
    with open(r_file) as rv_file:
        #print('inside review file')
        while True:
            starlist = list()
            line = rv_file.readline().strip()
            #print(line)
            #If EOF, break
            if line.strip() == '':
                break
            business_id = json.loads(line)["business_id"]
            stars = json.loads(line)['stars']
            #creating reviews dictionary
            if review.get(business_id) is None:
                review[business_id] = [stars]
            else:
                review[business_id].append(stars)
    #print('review dictionary --> ',review)
    #i=1
    # reading business file
    with open(b_file) as b_file:
        #print('inside business file')
        while True:
            #print(i)
            line = b_file.readline()
            #print(line)
            #i+=1
            if line.strip() == '':
                break
            categories = json.loads(line)['categories']
            if categories:
                business[json.loads(line)['business_id']] = [v.strip() for v in categories.split(',')]
    #print('business dictionary --> ',business)
    #print('read both files')
    for k in business.keys():
        if review.get(k) is not None:
            joined_dict[k] = {"stars":review[k], "categories":business[k]}
    #print('joined dictionary --> ',joined_dict)

    for k,v in joined_dict.items():
        for cat in v["categories"]:
            for star in v["stars"]:
                rating_list.append((cat,star))
    #print('rating list done')
    #print("rating list --> ", rating_list)

    for ratings in rating_list:
        if ratings[0] not in avg_dict.keys():
            avg_dict[ratings[0]] = {"sum":ratings[1],"count":1}
        else:
            avg_dict[ratings[0]]["sum"] += ratings[1]
            avg_dict[ratings[0]]["count"] += 1
    #print('computed avg dict()')
    #print("avg dict --> ",avg_dict)

    for k in avg_dict.keys():
        avg_list.append([k,avg_dict[k]["sum"]/avg_dict[k]["count"]])

    #print("final avg list --> ",avg_list)


    out_dict['result'] = sorted(avg_list, key=lambda i: (-i[1], i[0]))[:n_top]
    #print('sorted list', out_dict)


with open(o_file,'w') as out_file:
    json.dump(out_dict,out_file)

print('pyspark_time --> ',time.time()-stime)