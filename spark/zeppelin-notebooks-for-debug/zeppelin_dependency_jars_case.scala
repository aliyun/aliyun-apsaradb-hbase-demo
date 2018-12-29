

/**
  * 通过zeppelin调试时，如果要依赖第三方的jar包需要做以下步骤：
  *
  * 1、上传对应的三方jar包到httpfs，参考文档：https://help.aliyun.com/document_detail/99182.html。
  * 比如该例子，需要把example-dependency的jar包example-dependency-0.0.1-SNAPSHOT.jar上传，比如上传到/resourcesdir/example-dependency-0.0.1-SNAPSHOT.jar
  *
  * 2、更改zeppelin的Interpreters中livy Interpreter的配置，配置livy.spark.jars为 /resourcesdir/example-dependency-0.0.1-SNAPSHOT.jar
  *
  * 3、重启livy Interpreter
  *
  * 4、将下面的代码贴到zeppelin的livy notebook中可以加载到com.aliyun.spark.AddFunction并运行
  *
  * 5、其他jar上传类似，其中livy.spark.jars多个jar包使用逗号隔开
  */


%livy.spark
import spark.implicits._
import com.aliyun.spark.AddFunction

val data = Array (1, 2, 3, 4, 5)
val distData = spark.sparkContext.parallelize (data)
distData.map (s => AddFunction.add (s, 5) ).toDF.show()

