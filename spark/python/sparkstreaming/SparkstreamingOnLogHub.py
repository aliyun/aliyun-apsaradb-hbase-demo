from __future__ import print_function
import json
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from loghub import LoghubUtils

if __name__ == "__main__":
    if len(sys.argv) != 9:
        print >> sys.stderr, "Usage: spark-submit SparkstreamingOnLogHub.py logServiceProject logsStoreName " \
                             "logHubConsumerGroupName loghubEndpoint numReceiver accessKeyId accessKeySecret"
        exit(-1)

    sc = SparkContext(appName="PythonStreamingLoghubWordCount")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint(sys.argv[8])

    logServiceProject = sys.argv[1]
    logsStoreName = sys.argv[2]
    logHubConsumerGroupName = sys.argv[3]
    loghubEndpoint = sys.argv[4]
    numReceiver = int(sys.argv[5])
    accessKeyId = sys.argv[6]
    accessKeySecret = sys.argv[7]

    stream = LoghubUtils.createStream(ssc, logServiceProject, logsStoreName, logHubConsumerGroupName, loghubEndpoint,
                                       numReceiver, accessKeyId, accessKeySecret)
    f = lambda x: print(x)
    lines = stream.map(lambda x: json.loads(x))
    lines.foreachRDD(lambda rdd: rdd.foreach(lambda line: f(bytes('=') + line['c1'] + bytes('=') + line['c2'])))

    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a+b)
    # counts.pprint()

    ssc.start()
    ssc.awaitTermination()
