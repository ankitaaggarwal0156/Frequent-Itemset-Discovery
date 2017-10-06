import sys
from pyspark import SparkContext
from collections import defaultdict
sc = SparkContext()
s = float(sys.argv[2])
    
def getData(file1):
    dataset = list()
    itemset = set()
    for r in file1:
        t = frozenset(r)
        dataset.append(t)
        for item in t:
            itemset.add(frozenset([item]))         
    return itemset, dataset    


def getFreq(itemset, dataset,s, freq, num_lines):
    itemNew = set()
    local = defaultdict(int)
    for item in itemset:
        for t in dataset:
            if item.issubset(t):
                freq[item] += 1
                local[item] += 1
    for item, count in local.items():
        support = count
        if support >= s:
            itemNew.add(item)
    return itemNew
     
     
def Apriori(lines):
    t= list(lines)
    itemset, dataset = getData(t)
    num_lines=len(t)
    freq = defaultdict(int)
    s1 = s * num_lines
    itemNew = getFreq(itemset, dataset,s1, freq, num_lines)
    Output = dict()
    candidate = itemNew
    k = 2
    while(candidate != set([])):
        Output[k-1] = candidate
        candidate = join(candidate, k)
        frequent = getFreq(candidate,dataset,s1, freq,num_lines)
        candidate = frequent
        k = k + 1  
    for v in Output.values():
        for i in list(v):
            items.append(i)
    return items
    
       
def join(itemset, k):
    return set([i.union(j) for i in itemset for j in itemset if len(i.union(j)) == k]) 

def SONpass2(data1, items):
    t, dataset = getData(data1)
    num_lines=len(data1)
    freq = defaultdict(int)
    s1 = s * num_lines
    itemNew = getFreq(items, dataset,s1, freq, num_lines)
    out1=list()
    for i in itemNew:
        out1.append(list(i))
    return out1
        
    
lines = sc.textFile(sys.argv[1]).map(lambda x:list(x.split(","))).map(lambda x: list(int(y) for y in x))
num = lines.count()
filename = sys.argv[3]
f = open(filename,'w')
items = list()
it = lines.mapPartitions(Apriori).distinct()
dataset = lines.collect()
it1 = it.mapPartitions(lambda x: SONpass2(dataset,x))
output = it1.collect()
for v in output:
    o= ','.join(str(x) for x in v)
    f.write("%s\n" % (o))
f.close()

