

#1、first case

%livy.pyspark
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
print "%table pp"
for i in  distData.collect():
    print ("%s" %(i))


