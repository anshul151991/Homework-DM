from pyspark import SparkConf, SparkContext
import sys
import time
import itertools
from pyspark.sql import SparkSession

# print('System arguments --> ',sys.argv)
# Initializing input variables
stime = time.time()
out_dict = dict()
delm = ','
ct_dict = {}
case = int(sys.argv[1])
support = int(sys.argv[2])
i_file_loc = sys.argv[3]
o_file = sys.argv[4]


def aprioriImpl(part):
    # print(ct_dict)
    interim_list = list()
    partition_list = list(part)
    candidate_list = list()
    # print('partition -->', partition_list)

    # Checking the counts of items with k = 1
    for line in partition_list:
        for bid in line[1]:
            if ct_dict.get(bid):
                ct_dict[bid] += 1
            else:
                ct_dict[bid] = 1
    # print('for k=1 counts of elements --> ',ct_dict)

    # applying filtering on the basis of support

    for key in ct_dict.keys():
        if ct_dict[key] >= red_support:
            interim_list.append(key)
            candidate_list.append((key,))
    # print('for k=1 candidate items --> ',interim_list)

    k = 2
    while (len(interim_list) > 0):

        if k == 2:
            # print('Starting code for  k = 2')
            # print('interim_list-->', interim_list)
            pairs = [set(i) for i in (itertools.combinations(sorted(interim_list), k))]
            # print('Generated pairs', pairs)
        else:
            # print('interim_list-->', interim_list)
            pairs = list()
            # Generating Candidates for k > 2
            for i in range(0, len(interim_list)):
                for j in range(i + 1, len(interim_list)):
                    x = 0
                    intersection_pair = interim_list[i].intersection(interim_list[j])
                    if len(intersection_pair) == k - 2:
                        pair = interim_list[i].union(interim_list[j])
                        if pair not in pairs:
                            interim_pairs = list(itertools.combinations(pair, k - 1))
                            for l in interim_pairs:
                                if x == k - 2:
                                    break
                                if not intersection_pair.issubset(l):
                                    if set(l) in interim_list:
                                        x = x + 1
                            if x == k - 2:
                                pairs.append(pair)

        interim_list = list()
        for pair in pairs:
            count = 0
            # Generating counts of Pairs
            for bucket in partition_list:
                if count >= red_support:
                    break
                if set(pair).issubset((bucket[1])):
                    count += 1
                    # if count_pair.get(pair):
                    # count_pair[pair] += 1
                    ##print('adding 1')
                    # else:
                    # count_pair[pair] = 1
            if count >= red_support:
                interim_list.append(pair)
                candidate_list.append(tuple(sorted(pair)))
        k += 1

    # print('Candidate list combined for every k value --> ',candidate_list)
    return candidate_list


def secPhase(part):
    part_list = list(part)
    # print('Partition --> ',part_list)
    ct = {}
    for candidate in candidate_list:
        for line in part_list:
            if candidate in line[1] or set(candidate).issubset(set(line[1])):
                if ct.get(candidate):
                    ct[candidate] += 1
                else:
                    ct[candidate] = 1
    # print('ct dict for second phase -->',ct)
    return ct.items()


# Setting spark Session Initial Code
# conf = SparkConf().setAppName("task1").setMaster("local").set("spark.driver.memory", "4g")
# sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# load input file

r_file = sc.textFile(i_file_loc, 2)
# print('par -->',r_file.getNumPartitions())

# print('input file -->',r_file.collect())
# an = spark.read.csv(i_file) --> read file as DF

# remove header
hdr_rem_rdd = r_file.map(lambda line: line.split(delm)).filter(lambda x: x[0] != 'user_id')
# print('hdr_rem_rdd --> ', hdr_rem_rdd.collect())
# print(hdr_rem_rdd.count())

# basket creation
if case == 1:
    bsk_rdd = hdr_rem_rdd.groupByKey().map(lambda x: (x[0], set(x[1])))
elif case == 2:
    bsk_rdd = hdr_rem_rdd.map(lambda x: (x[1], x[0])).groupByKey().map(lambda x: (x[0], list(set(x[1]))))

n_part = bsk_rdd.getNumPartitions()
red_support = support / n_part
# print('Reduced support --> ',red_support)
# print('# of partitions --> ',n_part)
# print('basket --> ', bsk_rdd.collect())
data = bsk_rdd.mapPartitions(aprioriImpl)

candidate_list = set(data.collect())

# print(sorted(candidate_dict[2]))

# text_str+='Frequent Itemsets:' + '\n'
# print(text_str)


print('printing the number of partitions', bsk_rdd.count())
a = bsk_rdd.mapPartitions(secPhase).groupByKey().map(lambda x: (x[0], sum(x[1]))).filter(lambda x: x[1] >= support).map(
    lambda x: x[0]).collect()

# print('Candidate list --> ',candidate_list)
# Creating dictionary of candidate items with key as length
candidate_dict = dict()

for item in candidate_list:
    if len(item) not in candidate_dict.keys():
        candidate_dict[len(item)] = list()
    if isinstance(item, tuple):
        candidate_dict[len(item)].append(tuple(sorted(item)))
    else:
        candidate_dict[len(item)].append(item)
# print('candidate_dict --> ', candidate_dict)
text_str = "Candidates:\n"
for k, v in sorted(candidate_dict.items()):
    if k == 1:
        # text_str+=sorted(v)
        text_str += str(sorted(v)).replace(',), ', '),').replace(',)', ')').replace('[', '').replace(']', '') + '\n\n'
    else:
        text_str += str(sorted(v)).replace('), ', '),').replace('[', '').replace(']', '') + '\n\n'

frequent_dict = dict()
for item in a:
    if len(item) not in frequent_dict.keys():
        frequent_dict[len(item)] = list()
    if isinstance(item, tuple):
        frequent_dict[len(item)].append(tuple(sorted(item)))
    else:
        frequent_dict[len(item)].append(item)
# print('frequent_dict --> ', frequent_dict)

text_str += "Frequent Itemsets:" + '\n'
for k, v in sorted(frequent_dict.items()):
    if k == 1:
        # text_str+=sorted(v)
        text_str += str(sorted(v)).replace(',), ', '),').replace(',)', ')').replace('[', '').replace(']', '') + '\n\n'
    else:
        text_str += str(sorted(v)).replace('), ', '),').replace('[', '').replace(']', '') + '\n\n'
# print(text_str)

with open(o_file, 'w') as out_file:
    out_file.write(text_str.strip())

print('Duration: ', time.time() - stime)



