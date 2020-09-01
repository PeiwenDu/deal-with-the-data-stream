import sys,binascii,time
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext


class Bitmap:
    def __init__(self, max):
        self.size = int((max + 31 - 1) / 31)
        self.array = [0 for i in range(self.size)]

    def getElementIndex(self, num):
        return int(num / 31)


    def getBitIndex(self, num):
        return num % 31

    def set(self, num):
        elementIndex = self.getElementIndex(num)
        bitIndex = self.getBitIndex(num)
        element = self.array[elementIndex]
        self.array[elementIndex] = element | (1<<bitIndex)

    def test(self, num):
        elementIndex = self.getElementIndex(num)
        bitIndex = self.getBitIndex(num)
        if self.array[elementIndex] & (1 << bitIndex):
            return True
        return False

def findcity(map):
    for item in map:
        if "city" in item:
            return item

def hashfunction(city):
    result1 = (3 * city + 1)%200
    result2 = (7 * city + 2)%200
    return [result1, result2]

def checkrdd(rdd,flag,cityset,falsepostivenum,totalnum):
    estimateinstreambefore = True
    falsepostivenumnow =0
    for city in rdd.collect():
        if len(cityset) == 0:
            cityset.append(city)
            result = hashfunction(city)
            for re in result:
                flag.set(re)
        else:
            result = hashfunction(city)
            for re in result:
                if flag.test(re) == False:
                    estimateinstreambefore = False
                    break
            if estimateinstreambefore:
                if city not in cityset:
                    falsepostivenumnow +=1
                    cityset.append(city)
            else:
                cityset.append(city)
                for re in result:
                    flag.set(re)
    totalnum[-1] = totalnum[-1]+rdd.count()
    falsepostivenum[-1] = falsepostivenum[-1]+falsepostivenumnow
    falsepostiverate =float(falsepostivenum[-1]/totalnum[-1])
    # print(falsepostiverate)
    with open(outputfile, "a") as f:
        f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+","+str(falsepostiverate)+"\n")

port = int(sys.argv[1])
outputfile = sys.argv[2]
# port =9999
# outputfile = "output_task1"

# generate_stream ="/Users/peiwendu/Desktop/study/553/assignment/generate_stream.jar"
# businessjson = "/Users/peiwendu/Desktop/study/553/assignment/business.json"

conf = SparkConf().setAppName("inf553_hw6_task1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc=StreamingContext(sc, 10)
s = ssc.socketTextStream("localhost", port)

flag = Bitmap(200)
cityset = []
falsepostivenum = [0]
totalnum = [0]
with open(outputfile,"w") as f:
    f.write("Time,FPR\n")
cities = s.map(lambda x:x.split(",")).map(lambda x:findcity(x)).map(lambda x:x.split(":")[1]).map(lambda x:int(binascii.hexlify(x.encode('utf8')),16)).foreachRDD(lambda x:checkrdd(x,flag,cityset,falsepostivenum,totalnum))
ssc.start()
ssc.awaitTermination()







