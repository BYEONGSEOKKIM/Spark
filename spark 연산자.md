# distinct와 flatMap 변환 연산자





scala> val lines = sc.textFile("/home/lab02/client-ids.log")
lines: org.apache.spark.rdd.RDD[String] = /home/lab02/client-ids.log MapPartitionsRDD[4] at textFile at <console>:24

scala> val idsStr = lines.map(line => line.split(","))
idsStr: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[5] at map at <console>:25

scala> idsStr.foreach(println(_))
[Ljava.lang.String;@b073dff                                         (0 + 2) / 2]
[Ljava.lang.String;@7b806dfd
[Ljava.lang.String;@258c83be
[Ljava.lang.String;@16c2dbc3

scala> idsStr.first
res3: Array[String] = Array(15, 16, 20, 20)

scala> idsStr.collect
res4: Array[Array[String]] = Array(Array(15, 16, 20, 20), Array(77, 80, 94), Array(94, 98, 16, 31), Array(31, 15, 20))



#### 단일배열 분해



scala> val ids = lines.flatMap(_.split(","))
ids: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[6] at flatMap at <console>:25

scala> ids.collect
res5: Array[String] = Array(15, 16, 20, 20, 77, 80, 94, 94, 98, 16, 31, 31, 15, 20)

scala> ids.first
res6: String = 15

scala> ids.collect.mkString(";")
res7: String = 15;16;20;20;77;80;94;94;98;16;31;31;15;20



#### RDD의 요소를 string에서 int로 변환



scala> val intlds = ids.map(_.toInt)
intlds: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[7] at map at <console>:25

scala> intlds.collect
res8: Array[Int] = Array(15, 16, 20, 20, 77, 80, 94, 94, 98, 16, 31, 31, 15, 20)

scala> val uniquelds = intlds.distinct
uniquelds: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[10] at distinct at <console>:25

scala> uniquelds.collect
res9: Array[Int] = Array(16, 80, 98, 20, 94, 15, 77, 31)

scala> val finalCount = uniquelds.count
finalCount: Long = 8

scala> val transactionCount = ids.count
transactionCount: Long = 14

scala> val ids = lines.flatMap(_.split(","))
ids: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[11] at flatMap at <console>:25

scala> ids.count
res10: Long = 14

scala> val uniquelds = ids.distinct
uniquelds: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[14] at distinct at <console>:25

scala> uniquelds.count
res11: Long = 8

scala> uniquelds.collect
res12: Array[String] = Array(80, 20, 15, 31, 77, 98, 16, 94)