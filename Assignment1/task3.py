import json
import time
import sys
from pyspark import SparkConf, SparkContext

#setting input variables
stime = time.time()
r_file = sys.argv[1]
o_file = sys.argv[2]
part_type = sys.argv[3]
n_par = int(sys.argv[4])
n_rev = int(sys.argv[5])
out_dict = dict()

#creating hash partitioner function
def part_fn(business_id):
    return hash(business_id)

#setting spark context
conf = SparkConf().setAppName("task1").setMaster("local")
sc = SparkContext(conf=conf)

#reading input file
i_file = sc.textFile(r_file)

#print(i_file.getNumPartitions())

#For default partition
if part_type.lower() == 'default':
    i_data = i_file.map(lambda line : (json.loads(line)['business_id'].strip(),1))
    data = i_data.groupByKey().map(lambda x:[x[0],len(list(x[1]))]).filter(lambda x:x[1] > n_rev)
#For custom partition
elif part_type.lower() == 'customized':
    i_data = i_file.map(lambda line : (json.loads(line)['business_id'].strip(),1)).partitionBy(n_par, part_fn)
    data = i_data.groupByKey().map(lambda x: (x[0],len(list(x[1])))).filter(lambda x: x[1] > n_rev)

#setting output dictionary
out_dict['n_partitions'] = i_data.getNumPartitions()
out_dict['n_items'] = i_data.glom().map(len).collect()
out_dict['result'] = data.collect()

with open(o_file,'w') as out_file:
    json.dump(out_dict,out_file)
print('pyspark_time -->',time.time()-stime)