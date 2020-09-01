import sys,time
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

port = int(sys.argv[1])
outputfile = sys.argv[2]
# port =9999
# outputfile = "output_task2"


def findcity(map):
    for item in map:
        if "city" in item:
            return item

def findTrailingzero(num):
    if num == 0:
        return 0
    p = 0
    while (num >> p) & 1 == 0:
        p += 1
    return p

def hashfunction(city,primenum,length):
    return hash(city)%primenum

def checkrdd(cityrdd):
    inputcity = cityrdd.collect()
    uniqucities = []
    for city in inputcity:
        if city not in uniqucities:
            uniqucities.append(city)
    hashbox = dict()
    hashbox[0]=[]
    hashbox[1]=[]
    hashbox[2]=[]
    hashbox[3]=[]
    primenum = [401,317,19,53,59,61,103,229,109,383,443,571]
    for num in primenum:
        maxtail = 0
        for city in inputcity:
            maxtail = max([maxtail,findTrailingzero(hashfunction(city,num,len(uniqucities)))])
            # print(maxtail)
        hashbox[primenum.index(num)%4].append(2**maxtail)
    average =[]
    for i in hashbox:
        average.append(float(sum(hashbox[i])/len(hashbox[i])))
    average=sorted(average)
    r = (average[1]+average[2])/2
    # print(average)
    with open(outputfile,"a") as f:
        f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "," + str(len(uniqucities))+ "," +str(r) + "\n")


conf = SparkConf().setAppName("inf553_hw6_task2")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc=StreamingContext(sc, 5)
s = ssc.socketTextStream("localhost", port).window(30,10)


citys = s.map(lambda x:x.split(",")).map(lambda x:findcity(x)).map(lambda x:x.split(":")[1]).foreachRDD(lambda cityrdd:checkrdd(cityrdd))
with open(outputfile, "w") as f:
    f.write("Time,Ground Truth,Estimation\n")
ssc.start()
ssc.awaitTermination()



