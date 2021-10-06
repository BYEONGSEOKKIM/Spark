#### 설정



scala> import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

scala> val spark = SparkSession.builder().getOrCreate()
spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@85420

scala> import spark.implicits._
import spark.implicits._





#### 데이터 로드



scala> val itPostsRows = sc.textFile("italianPosts.csv")
itPostsRows: org.apache.spark.rdd.RDD[String] = italianPosts.csv MapPartitionsRDD[1] at textFile at <console>:30

scala> val itPostsSplit = itPostsRows.map(x => x.split("~"))
itPostsSplit: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[2] at map at <console>:31

scala> val itPostsRDD = itPostsSplit.map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))
itPostsRDD: org.apache.spark.rdd.RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)] = MapPartitionsRDD[3] at map at <console>:31

scala> val itPostsDFrame = itPostsRDD.toDF()
itPostsDFrame: org.apache.spark.sql.DataFrame = [_1: string, _2: string ... 11 more fields]

scala> itPostsDFrame.show(10)
+---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
| _1|                  _2| _3|                  _4| _5|                  _6|  _7|                  _8|                  _9| _10| _11|_12| _13|
+---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
|  4|2013-11-11 18:21:...| 17|&lt;p&gt;The infi...| 23|2013-11-10 19:37:...|null|                    |                    |null|null|  2|1165|
|  5|2013-11-10 20:31:...| 12|&lt;p&gt;Come cre...|  1|2013-11-10 19:44:...|  61|Cosa sapreste dir...| &lt;word-choice&gt;|   1|null|  1|1166|
|  2|2013-11-10 20:31:...| 17|&lt;p&gt;Il verbo...|  5|2013-11-10 19:58:...|null|                    |                    |null|null|  2|1167|
|  1|2014-07-25 13:15:...|154|&lt;p&gt;As part ...| 11|2013-11-10 22:03:...| 187|Ironic constructi...|&lt;english-compa...|   4|1170|  1|1168|
|  0|2013-11-10 22:15:...| 70|&lt;p&gt;&lt;em&g...|  3|2013-11-10 22:15:...|null|                    |                    |null|null|  2|1169|
|  2|2013-11-10 22:17:...| 17|&lt;p&gt;There's ...|  8|2013-11-10 22:17:...|null|                    |                    |null|null|  2|1170|
|  1|2013-11-11 09:51:...| 63|&lt;p&gt;As other...|  3|2013-11-11 09:51:...|null|                    |                    |null|null|  2|1171|
|  1|2013-11-12 23:57:...| 63|&lt;p&gt;The expr...|  1|2013-11-11 10:09:...|null|                    |                    |null|null|  2|1172|
|  9|2014-01-05 11:13:...| 63|&lt;p&gt;When I w...|  5|2013-11-11 10:28:...| 122|Is &quot;scancell...|&lt;usage&gt;&lt;...|   3|1181|  1|1173|
|  0|2013-11-11 10:58:...| 18|&lt;p&gt;Wow, wha...|  5|2013-11-11 10:58:...|null|                    |                    |null|null|  2|1174|
+---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
only showing top 10 rows


scala> itPostsRDD.toDF("commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")
res1: org.apache.spark.sql.DataFrame = [commentCount: string, lastActivityDate: string ... 11 more fields]



scala> val itPostsDF = itPostsRDD.toDF("commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")
itPostsDF: org.apache.spark.sql.DataFrame = [commentCount: string, lastActivityDate: string ... 11 more fields]

scala> itPostsDF.show(10)
+------------+--------------------+-----------+--------------------+-----+--------------------+---------+--------------------+--------------------+-----------+----------------+----------+----+
|commentCount|    lastActivityDate|ownerUserId|                body|score|        creationDate|viewCount|               title|                tags|answerCount|acceptedAnswerId|postTypeId|  id|
+------------+--------------------+-----------+--------------------+-----+--------------------+---------+--------------------+--------------------+-----------+----------------+----------+----+
|           4|2013-11-11 18:21:...|         17|&lt;p&gt;The infi...|   23|2013-11-10 19:37:...|     null|                    |                    |       null|            null|         2|1165|
|           5|2013-11-10 20:31:...|         12|&lt;p&gt;Come cre...|    1|2013-11-10 19:44:...|       61|Cosa sapreste dir...| &lt;word-choice&gt;|          1|            null|         1|1166|
|           2|2013-11-10 20:31:...|         17|&lt;p&gt;Il verbo...|    5|2013-11-10 19:58:...|     null|                    |                    |       null|            null|         2|1167|
|           1|2014-07-25 13:15:...|        154|&lt;p&gt;As part ...|   11|2013-11-10 22:03:...|      187|Ironic constructi...|&lt;english-compa...|          4|            1170|         1|1168|
|           0|2013-11-10 22:15:...|         70|&lt;p&gt;&lt;em&g...|    3|2013-11-10 22:15:...|     null|                    |                    |       null|            null|         2|1169|
|           2|2013-11-10 22:17:...|         17|&lt;p&gt;There's ...|    8|2013-11-10 22:17:...|     null|                    |                    |       null|            null|         2|1170|
|           1|2013-11-11 09:51:...|         63|&lt;p&gt;As other...|    3|2013-11-11 09:51:...|     null|                    |                    |       null|            null|         2|1171|
|           1|2013-11-12 23:57:...|         63|&lt;p&gt;The expr...|    1|2013-11-11 10:09:...|     null|                    |                    |       null|            null|         2|1172|
|           9|2014-01-05 11:13:...|         63|&lt;p&gt;When I w...|    5|2013-11-11 10:28:...|      122|Is &quot;scancell...|&lt;usage&gt;&lt;...|          3|            1181|         1|1173|
|           0|2013-11-11 10:58:...|         18|&lt;p&gt;Wow, wha...|    5|2013-11-11 10:58:...|     null|                    |                    |       null|            null|         2|1174|
+------------+--------------------+-----------+--------------------+-----+--------------------+---------+--------------------+--------------------+-----------+----------------+----------+----+
only showing top 10 rows

scala> itPostsDF.printSchema
root
 |-- commentCount: string (nullable = true)
 |-- lastActivityDate: string (nullable = true)
 |-- ownerUserId: string (nullable = true)
 |-- body: string (nullable = true)
 |-- score: string (nullable = true)
 |-- creationDate: string (nullable = true)
 |-- viewCount: string (nullable = true)
 |-- title: string (nullable = true)
 |-- tags: string (nullable = true)
 |-- answerCount: string (nullable = true)
 |-- acceptedAnswerId: string (nullable = true)
 |-- postTypeId: string (nullable = true)
 |-- id: string (nullable = true)





#### 케이스 클래스를 사용해 RDD를 DataFrame으로 변환




scala> import java.sql.Timestamp
import java.sql.Timestamp

scala> case class Post (commentCount:Option[Int], lastActivityDate:Option[java.sql.Timestamp],
     |   ownerUserId:Option[Long], body:String, score:Option[Int], creationDate:Option[java.sql.Timestamp],
     |   viewCount:Option[Int], title:String, tags:String, answerCount:Option[Int],
     |   acceptedAnswerId:Option[Long], postTypeId:Option[Long], id:Long)
defined class Post

scala> object StringImplicits {
     |    implicit class StringImprovements(val s: String) {
     |       import scala.util.control.Exception.catching
     |       def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
     |       def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
     |       def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
     |    }
     | }
defined object StringImplicits

scala> import StringImplicits._
import StringImplicits._

scala> def stringToPost(row:String):Post = {
     |   val r = row.split("~")
     |   Post(r(0).toIntSafe,
     |     r(1).toTimestampSafe,
     |     r(2).toLongSafe,
     |     r(3),
     |     r(4).toIntSafe,
     |     r(5).toTimestampSafe,
     |     r(6).toIntSafe,
     |     r(7),
     |     r(8),
     |     r(9).toIntSafe,
     |     r(10).toLongSafe,
     |     r(11).toLongSafe,
     |     r(12).toLong)
     | }
stringToPost: (row: String)Post

scala> val itPostsDFCase = itPostsRows.map(x => stringToPost(x)).toDF()
itPostsDFCase: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]

scala> itPostsDFCase.printSchema
root
 |-- commentCount: integer (nullable = true)
 |-- lastActivityDate: timestamp (nullable = true)
 |-- ownerUserId: long (nullable = true)
 |-- body: string (nullable = true)
 |-- score: integer (nullable = true)
 |-- creationDate: timestamp (nullable = true)
 |-- viewCount: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- tags: string (nullable = true)
 |-- answerCount: integer (nullable = true)
 |-- acceptedAnswerId: long (nullable = true)
 |-- postTypeId: long (nullable = true)
 |-- id: long (nullable = false)





#### 스키마를 지정해 RDD를 DataFrame으로 변환



scala> import org.apache.spark.sql.types._
import org.apache.spark.sql.types._

scala> val postSchema = StructType(Seq(
     |   StructField("commentCount", IntegerType, true),
     |   StructField("lastActivityDate", TimestampType, true),
     |   StructField("ownerUserId", LongType, true),
     |   StructField("body", StringType, true),
     |   StructField("score", IntegerType, true),
     |   StructField("creationDate", TimestampType, true),
     |   StructField("viewCount", IntegerType, true),
     |   StructField("title", StringType, true),
     |   StructField("tags", StringType, true),
     |   StructField("answerCount", IntegerType, true),
     |   StructField("acceptedAnswerId", LongType, true),
     |   StructField("postTypeId", LongType, true),
     |   StructField("id", LongType, false))
     |   )
postSchema: org.apache.spark.sql.types.StructType = StructType(StructField(commentCount,IntegerType,true), StructField(lastActivityDate,TimestampType,true), StructField(ownerUserId,LongType,true), StructField(body,StringType,true), StructField(score,IntegerType,true), StructField(creationDate,TimestampType,true), StructField(viewCount,IntegerType,true), StructField(title,StringType,true), StructField(tags,StringType,true), StructField(answerCount,IntegerType,true), StructField(acceptedAnswerId,LongType,true), StructField(postTypeId,LongType,true), StructField(id,LongType,false))

scala> import org.apache.spark.sql.Row
import org.apache.spark.sql.Row

scala> def stringToRow(row:String):Row = {
     |   val r = row.split("~")
     |   Row(r(0).toIntSafe.getOrElse(null),
     |     r(1).toTimestampSafe.getOrElse(null),
     |     r(2).toLongSafe.getOrElse(null),
     |     r(3),
     |     r(4).toIntSafe.getOrElse(null),
     |     r(5).toTimestampSafe.getOrElse(null),
     |     r(6).toIntSafe.getOrElse(null),
     |     r(7),
     |     r(8),
     |     r(9).toIntSafe.getOrElse(null),
     |     r(10).toLongSafe.getOrElse(null),
     |     r(11).toLongSafe.getOrElse(null),
     |     r(12).toLong)
     | }
stringToRow: (row: String)org.apache.spark.sql.Row

scala> val rowRDD = itPostsRows.map(row => stringToRow(row))
rowRDD: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[11] at map at <console>:43

scala> val itPostsDFStruct = spark.createDataFrame(rowRDD, postSchema)
itPostsDFStruct: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]





#### 스키마 정보 가져오기



scala> itPostsDFStruct.columns
res7: Array[String] = Array(commentCount, lastActivityDate, ownerUserId, body, score, creationDate, viewCount, title, tags, answerCount, acceptedAnswerId, postTypeId, id)

scala> itPostsDFStruct.dtypes
res8: Array[(String, String)] = Array((commentCount,IntegerType), (lastActivityDate,TimestampType), (ownerUserId,LongType), (body,StringType), (score,IntegerType), (creationDate,TimestampType), (viewCount,IntegerType), (title,StringType), (tags,StringType), (answerCount,IntegerType), (acceptedAnswerId,LongType), (postTypeId,LongType), (id,LongType))

scala> itPostsDFStruct.dtypes
res9: Array[(String, String)] = Array((commentCount,IntegerType), (lastActivityDate,TimestampType), (ownerUserId,LongType), (body,StringType), (score,IntegerType), (creationDate,TimestampType), (viewCount,IntegerType), (title,StringType), (tags,StringType), (answerCount,IntegerType), (acceptedAnswerId,LongType), (postTypeId,LongType), (id,LongType))

scala> val postsDf = itPostsDFStruct
postsDf: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]

scala> val postsIdBody = postsDf.select("id", "body")
postsIdBody: org.apache.spark.sql.DataFrame = [id: bigint, body: string]

scala> val postsIdBody = postsDf.select(postsDf.col("id"), postsDf.col("body"))
postsIdBody: org.apache.spark.sql.DataFrame = [id: bigint, body: string]

scala> val postsIdBody = postsDf.select(Symbol("id"), Symbol("body"))
postsIdBody: org.apache.spark.sql.DataFrame = [id: bigint, body: string]

scala> val postsIdBody = postsDf.select('id, 'body)
postsIdBody: org.apache.spark.sql.DataFrame = [id: bigint, body: string]

scala> val postsIdBody = postsDf.select($"id", $"body")
postsIdBody: org.apache.spark.sql.DataFrame = [id: bigint, body: string]

scala> val postIds = postsIdBody.drop("body")
postIds: org.apache.spark.sql.DataFrame = [id: bigint]





#### 데이터 필터링



scala> postsIdBody.filter('body contains "Italiano").count()
res10: Long = 46

scala> val noAnswer = postsDf.filter(('postTypeId === 1) and ('acceptedAnswerId isNull))
warning: there was one feature warning; for details, enable `:setting -feature' or `:replay -feature'
noAnswer: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]

scala> val firstTenQs = postsDf.filter('postTypeId === 1).limit(10)
firstTenQs: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]





#### 칼럼을 추가하거나 칼럼 이름 변경



scala> val firstTenQsRn = firstTenQs.withColumnRenamed("ownerUserId", "owner")
firstTenQsRn: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]

scala> postsDf.filter('postTypeId === 1).withColumn("ratio", 'viewCount / 'score).where('ratio < 35).show()
+------------+--------------------+-----------+--------------------+-----+--------------------+---------+--------------------+--------------------+-----------+----------------+----------+----+-------------------+
|commentCount|    lastActivityDate|ownerUserId|                body|score|        creationDate|viewCount|               title|                tags|answerCount|acceptedAnswerId|postTypeId|  id|              ratio|
+------------+--------------------+-----------+--------------------+-----+--------------------+---------+--------------------+--------------------+-----------+----------------+----------+----+-------------------+
|           1|2014-07-25 13:15:...|        154|&lt;p&gt;As part ...|   11|2013-11-10 22:03:...|      187|Ironic constructi...|&lt;english-compa...|          4|            1170|         1|1168|               17.0|
|           9|2014-01-05 11:13:...|         63|&lt;p&gt;When I w...|    5|2013-11-11 10:28:...|      122|Is &quot;scancell...|&lt;usage&gt;&lt;...|          3|            1181|         1|1173|               24.4|
|           1|2014-01-16 19:56:...|         63|&lt;p&gt;Suppose ...|    4|2013-11-11 11:31:...|      114|How should I tran...|&lt;usage&gt;&lt;...|          2|            1177|         1|1175|               28.5|
|           0|2013-11-11 14:36:...|         63|&lt;p&gt;Except w...|    3|2013-11-11 11:39:...|       58|Using a comma bet...|&lt;usage&gt;&lt;...|          2|            1182|         1|1176| 19.333333333333332|
|           0|2013-11-12 11:24:...|         63|&lt;p&gt;Comparin...|    3|2013-11-11 12:58:...|       60|Using the conditi...|&lt;usage&gt;&lt;...|          2|            1180|         1|1179|               20.0|
|           2|2013-11-11 23:23:...|        159|&lt;p&gt;Sono un'...|    7|2013-11-11 18:19:...|      138|origine dell'espr...|&lt;idioms&gt;&lt...|          2|            1185|         1|1184| 19.714285714285715|
|           5|2013-11-21 14:04:...|          8|&lt;p&gt;The use ...|   13|2013-11-11 21:01:...|      142|Usage of preposit...|&lt;prepositions&...|          2|            1212|         1|1192| 10.923076923076923|
|           0|2013-11-12 09:26:...|         17|&lt;p&gt;When wri...|    5|2013-11-11 21:01:...|       70|What's the correc...|&lt;punctuation&g...|          4|            1195|         1|1193|               14.0|
|           1|2013-11-24 09:35:...|         12|&lt;p&gt;Are &quo...|    8|2013-11-11 21:44:...|      135|Are &quot;qui&quo...|     &lt;grammar&gt;|          2|            null|         1|1197|             16.875|
|           0|2013-11-12 10:32:...|         63|&lt;p&gt;When wri...|    1|2013-11-12 09:31:...|       34|Period after abbr...|&lt;usage&gt;&lt;...|          1|            1202|         1|1201|               34.0|
|           1|2013-11-12 12:53:...|         99|&lt;p&gt;I can't ...|   -3|2013-11-12 10:57:...|       68|Past participle o...|&lt;grammar&gt;&l...|          3|            1216|         1|1203|-22.666666666666668|
|           4|2014-09-09 08:54:...|         63|&lt;p&gt;Some wor...|    4|2013-11-12 11:03:...|       80|When is the lette...|&lt;nouns&gt;&lt;...|          2|            1207|         1|1205|               20.0|
|           1|2013-11-13 18:45:...|         12|&lt;p&gt;Sapreste...|    4|2013-11-12 13:10:...|       82|Quali sono gli er...|     &lt;grammar&gt;|          2|            null|         1|1217|               20.5|
|           6|2014-04-09 18:02:...|         63|&lt;p&gt;In Lomba...|    5|2013-11-12 13:12:...|      100|Is &quot;essere d...|&lt;usage&gt;&lt;...|          4|            1226|         1|1218|               20.0|
|           3|2014-09-11 14:37:...|         63|&lt;p&gt;The plur...|    5|2013-11-12 13:34:...|       59|Why is the plural...|&lt;plural&gt;&lt...|          1|            1227|         1|1221|               11.8|
|           1|2013-11-12 13:49:...|         63|&lt;p&gt;I rememb...|    6|2013-11-12 13:38:...|       53|Can &quot;sciò&qu...|&lt;usage&gt;&lt;...|          1|            1223|         1|1222|  8.833333333333334|
|           2|2014-08-14 17:11:...|         99|&lt;p&gt;Is it ma...|    7|2013-11-12 14:11:...|      163|Usage of accented...|&lt;word-choice&g...|          2|            1228|         1|1224| 23.285714285714285|
|           2|2013-11-14 11:50:...|         18|&lt;p&gt;Where do...|    5|2013-11-12 14:49:...|      123|Etymology of &quo...|&lt;idioms&gt;&lt...|          1|            1230|         1|1229|               24.6|
|           0|2013-11-12 18:50:...|         53|&lt;p&gt;Do all I...|    4|2013-11-12 15:38:...|       62|Do adjectives alw...|&lt;adjectives&gt...|          2|            1234|         1|1231|               15.5|
|           5|2014-04-26 17:30:...|        132|&lt;p&gt;In Itali...|    7|2013-11-12 20:01:...|      230|&quot;Darsi del t...|&lt;usage&gt;&lt;...|          4|            null|         1|1238| 32.857142857142854|
+------------+--------------------+-----------+--------------------+-----+--------------------+---------+--------------------+--------------------+-----------+----------------+----------+----+-------------------+
only showing top 20 rows

scala> postsDf.filter('postTypeId === 1).orderBy('lastActivityDate desc).limit(10).show
warning: there was one feature warning; for details, enable `:setting -feature' or `:replay -feature'
+------------+--------------------+-----------+--------------------+-----+--------------------+---------+--------------------+--------------------+-----------+----------------+----------+----+
|commentCount|    lastActivityDate|ownerUserId|                body|score|        creationDate|viewCount|               title|                tags|answerCount|acceptedAnswerId|postTypeId|  id|
+------------+--------------------+-----------+--------------------+-----+--------------------+---------+--------------------+--------------------+-----------+----------------+----------+----+
|           2|2014-09-13 22:25:...|        707|&lt;p&gt;Ho osser...|    3|2014-09-12 09:44:...|      102|Perché a volte si...|&lt;orthography&g...|          1|            2344|         1|2343|
|           9|2014-09-13 13:40:...|        193|&lt;p&gt;I would ...|    4|2014-09-09 12:34:...|       88|Essential list of...|&lt;vocabulary&gt...|          1|            null|         1|2330|
|           0|2014-09-13 13:29:...|        707|&lt;p&gt;So che l...|    3|2014-09-13 08:55:...|       47|Perché si chiama ...|   &lt;etymology&gt;|          1|            null|         1|2345|
|           0|2014-09-13 12:43:...|        193|&lt;p&gt;Ho letto...|    5|2014-09-13 12:43:...|       21|Verbo impersonale...|&lt;grammar&gt;&l...|          0|            null|         1|2346|
|           0|2014-09-12 10:55:...|          8|&lt;p&gt;Wikipedi...|   10|2013-11-20 16:42:...|      145|Plural form of &q...|&lt;orthography&g...|          5|            1336|         1|1321|
|           8|2014-09-12 10:55:...|         12|&lt;p&gt;Perché '...|    0|2014-02-15 14:25:...|      144|Perché 'di per sè...|&lt;orthography&g...|          3|            null|         1|1696|
|           1|2014-09-12 07:32:...|        523|&lt;p&gt;After li...|    2|2014-09-11 17:57:...|       40|“Che t'aggia di'”...|&lt;word-meaning&...|          1|            2342|         1|2341|
|           0|2014-09-11 20:33:...|        765|&lt;p&gt;Alcune p...|    4|2014-07-28 18:45:...|       66|Cambio genere al ...|&lt;etymology&gt;...|          1|            2134|         1|2133|
|           0|2014-09-11 16:03:...|        707|&lt;p&gt;Sapresti...|    4|2014-09-11 14:45:...|       20|Origine di &quot;...|   &lt;etymology&gt;|          1|            2340|         1|2339|
|           3|2014-09-11 14:37:...|         63|&lt;p&gt;The plur...|    5|2013-11-12 13:34:...|       59|Why is the plural...|&lt;plural&gt;&lt...|          1|            1227|         1|1221|
+------------+--------------------+-----------+--------------------+-----+--------------------+---------+--------------------+--------------------+-----------+----------------+----------+----+





#### SQL 함수로 데이터에 연산 수행




scala> import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

scala> postsDf.filter('postTypeId === 1).withColumn("activePeriod", datediff('lastActivityDate, 'creationDate)).orderBy('activePeriod desc).head.getString(3).replace("&lt;","<").replace("&gt;",">")
warning: there was one feature warning; for details, enable `:setting -feature' or `:replay -feature'
res13: String = <p>The plural of <em>braccio</em> is <em>braccia</em>, and the plural of <em>avambraccio</em> is <em>avambracci</em>.</p><p>Why are the plural of those words so different, if they both are referring to parts of the human body, and <em>avambraccio</em> derives from <em>braccio</em>?</p>

scala> postsDf.select(avg('score), max('score), count('score)).show
+-----------------+----------+------------+
|       avg(score)|max(score)|count(score)|
+-----------------+----------+------------+
|4.159397303727201|        24|        1261|
+-----------------+----------+------------+





#### 윈도 함수




scala> import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window

scala> postsDf.filter('postTypeId === 1).select('ownerUserId, 'acceptedAnswerId, 'score, max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser").withColumn("toMax", 'maxPerUser - 'score).show(10)
+-----------+----------------+-----+----------+-----+
|ownerUserId|acceptedAnswerId|score|maxPerUser|toMax|
+-----------+----------------+-----+----------+-----+
|        348|            1570|    5|         5|    0|
|        736|            null|    1|         1|    0|
|         22|            1263|    6|        12|    6|
|         22|            null|    6|        12|    6|
|         22|            1293|   12|        12|    0|
|         22|            null|    6|        12|    6|
|         22|            1338|    5|        12|    7|
|         22|            1408|    3|        12|    9|
|         22|            1379|    5|        12|    7|
|         22|            1411|    5|        12|    7|
+-----------+----------------+-----+----------+-----+
only showing top 10 rows


scala> postsDf.select(avg('score), max('score), count('score)).show
+-----------------+----------+------------+
|       avg(score)|max(score)|count(score)|
+-----------------+----------+------------+
|4.159397303727201|        24|        1261|
+-----------------+----------+------------+


scala> import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window

scala> postsDf.filter('postTypeId === 1).select('ownerUserId, 'acceptedAnswerId, 'score, max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser").withColumn("toMax", 'maxPerUser - 'score).show(10)
+-----------+----------------+-----+----------+-----+
|ownerUserId|acceptedAnswerId|score|maxPerUser|toMax|
+-----------+----------------+-----+----------+-----+
|        348|            1570|    5|         5|    0|
|        736|            null|    1|         1|    0|
|         22|            1263|    6|        12|    6|
|         22|            null|    6|        12|    6|
|         22|            1293|   12|        12|    0|
|         22|            null|    6|        12|    6|
|         22|            1338|    5|        12|    7|
|         22|            1408|    3|        12|    9|
|         22|            1379|    5|        12|    7|
|         22|            1411|    5|        12|    7|
+-----------+----------------+-----+----------+-----+
only showing top 10 rows


scala> postsDf.filter('postTypeId === 1).select('ownerUserId, 'id, 'creationDate, lag('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "prev", lead('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "next").orderBy('ownerUserId, 'id).show()
+-----------+----+--------------------+----+----+
|ownerUserId|  id|        creationDate|prev|next|
+-----------+----+--------------------+----+----+
|          4|1637|2014-01-24 06:51:...|null|null|
|          8|   1|2013-11-05 20:22:...|null| 112|
|          8| 112|2013-11-08 13:14:...|   1|1192|
|          8|1192|2013-11-11 21:01:...| 112|1276|
|          8|1276|2013-11-15 16:09:...|1192|1321|
|          8|1321|2013-11-20 16:42:...|1276|1365|
|          8|1365|2013-11-23 09:09:...|1321|null|
|         12|  11|2013-11-05 21:30:...|null|  17|
|         12|  17|2013-11-05 22:17:...|  11|  18|
|         12|  18|2013-11-05 22:34:...|  17|  19|
|         12|  19|2013-11-05 22:38:...|  18|  63|
|         12|  63|2013-11-06 17:54:...|  19|  65|
|         12|  65|2013-11-06 18:07:...|  63|  69|
|         12|  69|2013-11-06 19:41:...|  65|  70|
|         12|  70|2013-11-06 20:35:...|  69|  89|
|         12|  89|2013-11-07 19:22:...|  70|  94|
|         12|  94|2013-11-07 20:42:...|  89| 107|
|         12| 107|2013-11-08 08:27:...|  94| 122|
|         12| 122|2013-11-08 20:55:...| 107|1141|
|         12|1141|2013-11-09 20:50:...| 122|1142|
+-----------+----+--------------------+----+----+
only showing top 20 rows


scala> val countTags = udf((tags: String) => "&lt;".r.findAllMatchIn(tags).length)
countTags: org.apache.spark.sql.expressions.UserDefinedFunction = SparkUserDefinedFunction($Lambda$4220/1687901764@6bc24e94,IntegerType,List(Some(class[value[0]: string])),Some(class[value[0]: int]),None,false,true)



#### 사용자 정의 함수



scala> val countTags = spark.udf.register("countTags", (tags: String) => "&lt;".r.findAllMatchIn(tags).length)
countTags: org.apache.spark.sql.expressions.UserDefinedFunction = SparkUserDefinedFunction($Lambda$4225/2072802374@5e413b47,IntegerType,List(Some(class[value[0]: string])),Some(class[value[0]: int]),Some(countTags),false,true)

scala> postsDf.filter('postTypeId === 1).select('tags, countTags('tags) as "tagCnt").show(10, false)
+-------------------------------------------------------------------+------+
|tags                                                               |tagCnt|
+-------------------------------------------------------------------+------+
|&lt;word-choice&gt;                                                |1     |
|&lt;english-comparison&gt;&lt;translation&gt;&lt;phrase-request&gt;|3     |
|&lt;usage&gt;&lt;verbs&gt;                                         |2     |
|&lt;usage&gt;&lt;tenses&gt;&lt;english-comparison&gt;              |3     |
|&lt;usage&gt;&lt;punctuation&gt;                                   |2     |
|&lt;usage&gt;&lt;tenses&gt;                                        |2     |
|&lt;history&gt;&lt;english-comparison&gt;                          |2     |
|&lt;idioms&gt;&lt;etymology&gt;                                    |2     |
|&lt;idioms&gt;&lt;regional&gt;                                     |2     |
|&lt;grammar&gt;                                                    |1     |
+-------------------------------------------------------------------+------+
only showing top 10 rows





#### 결측 값 다루기




scala> val cleanPosts = postsDf.na.drop()
cleanPosts: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]

scala> cleanPosts.count()
res20: Long = 222

scala> postsDf.na.fill(Map("viewCount" -> 0))
res21: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]

scala> val postsDfCorrected = postsDf.na.replace(Array("id", "acceptedAnswerId"), Map(1177 -> 3000))
postsDfCorrected: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]





#### DataFrame을 RDD로 변환



scala> val postsRdd = postsDf.rdd
postsRdd: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[72] at rdd at <console>:47

scala> val postsMapped = postsDf.rdd.map(row => Row.fromSeq(
     |   row.toSeq.updated(3, row.getString(3).replace("&lt;","<").replace("&gt;",">")).
     |     updated(8, row.getString(8).replace("&lt;","<").replace("&gt;",">"))))
postsMapped: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[73] at map at <console>:47

scala> val postsDfNew = spark.createDataFrame(postsMapped, postsDf.schema)
postsDfNew: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]





#### 데이터 그룹핑



scala> postsDfNew.groupBy('ownerUserId, 'tags, 'postTypeId).count.orderBy('ownerUserId desc).show(10)
warning: there was one feature warning; for details, enable `:setting -feature' or `:replay -feature'
+-----------+--------------------+----------+-----+
|ownerUserId|                tags|postTypeId|count|
+-----------+--------------------+----------+-----+
|        862|                    |         2|    1|
|        855|         <resources>|         1|    1|
|        846|<translation><eng...|         1|    1|
|        845|<word-meaning><tr...|         1|    1|
|        842|  <verbs><resources>|         1|    1|
|        835|    <grammar><verbs>|         1|    1|
|        833|<meaning><article...|         1|    1|
|        833|           <meaning>|         1|    1|
|        833|                    |         2|    1|
|        814|                    |         2|    1|
+-----------+--------------------+----------+-----+
only showing top 10 rows


scala> postsDfNew.groupBy('ownerUserId).agg(max('lastActivityDate), max('score)).show(10)
+-----------+---------------------+----------+
|ownerUserId|max(lastActivityDate)|max(score)|
+-----------+---------------------+----------+
|        270| 2014-02-25 17:43:...|         1|
|        730| 2014-07-12 00:58:...|        12|
|        720| 2014-07-07 21:33:...|         1|
|         19| 2013-11-27 14:21:...|        10|
|        348| 2014-01-06 13:00:...|         5|
|        415| 2014-08-25 00:23:...|         5|
|        656| 2014-05-27 19:30:...|         9|
|        736| 2014-07-15 11:09:...|         1|
|         22| 2014-09-10 07:15:...|        19|
|        198| 2013-12-18 15:57:...|         5|
+-----------+---------------------+----------+
only showing top 10 rows


scala> postsDfNew.groupBy('ownerUserId).agg(Map("lastActivityDate" -> "max", "score" -> "max")).show(10)
+-----------+---------------------+----------+
|ownerUserId|max(lastActivityDate)|max(score)|
+-----------+---------------------+----------+
|        270| 2014-02-25 17:43:...|         1|
|        730| 2014-07-12 00:58:...|        12|
|        720| 2014-07-07 21:33:...|         1|
|         19| 2013-11-27 14:21:...|        10|
|        348| 2014-01-06 13:00:...|         5|
|        415| 2014-08-25 00:23:...|         5|
|        656| 2014-05-27 19:30:...|         9|
|        736| 2014-07-15 11:09:...|         1|
|         22| 2014-09-10 07:15:...|        19|
|        198| 2013-12-18 15:57:...|         5|
+-----------+---------------------+----------+
only showing top 10 rows


scala> postsDfNew.groupBy('ownerUserId).agg(max('lastActivityDate), max('score).gt(5)).show(10)
+-----------+---------------------+----------------+
|ownerUserId|max(lastActivityDate)|(max(score) > 5)|
+-----------+---------------------+----------------+
|        270| 2014-02-25 17:43:...|           false|
|        730| 2014-07-12 00:58:...|            true|
|        720| 2014-07-07 21:33:...|           false|
|         19| 2013-11-27 14:21:...|            true|
|        348| 2014-01-06 13:00:...|           false|
|        415| 2014-08-25 00:23:...|           false|
|        656| 2014-05-27 19:30:...|            true|
|        736| 2014-07-15 11:09:...|           false|
|         22| 2014-09-10 07:15:...|            true|
|        198| 2013-12-18 15:57:...|           false|
+-----------+---------------------+----------------+
only showing top 10 rows


scala> val smplDf = postsDfNew.where('ownerUserId >= 13 and 'ownerUserId <= 15)
smplDf: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]

scala> smplDf.groupBy('ownerUserId, 'tags, 'postTypeId).count.show()
+-----------+----+----------+-----+
|ownerUserId|tags|postTypeId|count|
+-----------+----+----------+-----+
|         13|    |         2|    1|
|         14|    |         2|    2|
|         15|    |         2|    2|
+-----------+----+----------+-----+


scala>

scala> smplDf.rollup('ownerUserId, 'tags, 'postTypeId).count.show()
+-----------+----+----------+-----+
|ownerUserId|tags|postTypeId|count|
+-----------+----+----------+-----+
|       null|null|      null|    5|
|         13|    |         2|    1|
|         15|    |         2|    2|
|         15|    |      null|    2|
|         14|    |      null|    2|
|         15|null|      null|    2|
|         14|null|      null|    2|
|         13|null|      null|    1|
|         14|    |         2|    2|
|         13|    |      null|    1|
+-----------+----+----------+-----+


scala> smplDf.cube('ownerUserId, 'tags, 'postTypeId).count.show()
+-----------+----+----------+-----+
|ownerUserId|tags|postTypeId|count|
+-----------+----+----------+-----+
|       null|    |         2|    5|
|       null|null|         2|    5|
|       null|null|      null|    5|
|         13|    |         2|    1|
|         15|    |         2|    2|
|         13|null|         2|    1|
|         15|    |      null|    2|
|       null|    |      null|    5|
|         14|    |      null|    2|
|         15|null|      null|    2|
|         14|null|      null|    2|
|         13|null|      null|    1|
|         15|null|         2|    2|
|         14|    |         2|    2|
|         14|null|         2|    2|
|         13|    |      null|    1|
+-----------+----+----------+-----+


scala> spark.sql("SET spark.sql.caseSensitive=true")
res29: org.apache.spark.sql.DataFrame = [key: string, value: string]

scala> spark.conf.set("spark.sql.caseSensitive", "true")

scala> spark.conf.set("spark.sql.caseSensitive", "true")





#### 데이터 조인



scala> val itVotesRaw = sc.textFile("italianVotes.csv").map(x => x.split("~"))
itVotesRaw: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[113] at map at <console>:46

scala> val itVotesRows = itVotesRaw.map(row => Row(row(0).toLong, row(1).toLong, row(2).toInt, Timestamp.valueOf(row(3))))
itVotesRows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[114] at map at <console>:47

scala> val votesSchema = StructType(Seq(
     |   StructField("id", LongType, false),
     |   StructField("postId", LongType, false),
     |   StructField("voteTypeId", IntegerType, false),
     |   StructField("creationDate", TimestampType, false))
     |   )
votesSchema: org.apache.spark.sql.types.StructType = StructType(StructField(id,LongType,false), StructField(postId,LongType,false), StructField(voteTypeId,IntegerType,false), StructField(creationDate,TimestampType,false))

scala> val votesDf = spark.createDataFrame(itVotesRows, votesSchema)
votesDf: org.apache.spark.sql.DataFrame = [id: bigint, postId: bigint ... 2 more fields]

scala> val postsVotes = postsDf.join(votesDf, postsDf("id") === votesDf("postId"))
postsVotes: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 15 more fields]

scala> val postsVotesOuter = postsDf.join(votesDf, postsDf("id") === votesDf("postId"), "outer")
postsVotesOuter: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 15 more fields]





#### 테이블 카탈로그와 하이브 매타스토어



scala> postsDf.createOrReplaceTempView("posts_temp")

scala> postsDf.write.saveAsTable("posts")
21/10/04 17:43:42 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
21/10/04 17:43:42 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
21/10/04 17:43:42 WARN General: Plugin (Bundle) "org.datanucleus.store.rdbms" is already registered. Ensure you dont have multiple JAR versions of the same plugin in the classpath. The URL "file:/opt/spark-3.1.2-bin-hadoop3.2/jars/datanucleus-rdbms-4.1.19.jar" is already registered, and you are trying to register an identical plugin located at URL "file:/opt/spark/jars/datanucleus-rdbms-4.1.19.jar."
21/10/04 17:43:42 WARN General: Plugin (Bundle) "org.datanucleus" is already registered. Ensure you dont have multiple JAR versions of the same plugin in the classpath. The URL "file:/opt/spark-3.1.2-bin-hadoop3.2/jars/datanucleus-core-4.1.17.jar" is already registered, and you are trying to register an identical plugin located at URL "file:/opt/spark/jars/datanucleus-core-4.1.17.jar."
21/10/04 17:43:42 WARN General: Plugin (Bundle) "org.datanucleus.api.jdo" is already registered. Ensure you dont have multiple JAR versions of the same plugin in the classpath. The URL "file:/opt/spark-3.1.2-bin-hadoop3.2/jars/datanucleus-api-jdo-4.2.4.jar" is already registered, and you are trying to register an identical plugin located at URL "file:/opt/spark/jars/datanucleus-api-jdo-4.2.4.jar."
21/10/04 17:43:48 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
21/10/04 17:43:48 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore lab02@172.31.44.193
21/10/04 17:43:48 WARN ObjectStore: Failed to get database default, returning NoSuchObjectException
21/10/04 17:43:51 WARN SessionState: METASTORE_FILTER_HOOK will be ignored, since hive.security.authorization.manager is set to instance of HiveAuthorizerFactory.
21/10/04 17:43:51 WARN HiveConf: HiveConf of name hive.internal.ss.authz.settings.applied.marker does not exist
21/10/04 17:43:51 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
21/10/04 17:43:51 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist

scala> votesDf.write.saveAsTable("votes")

scala> spark.catalog.listTables().show()
21/10/04 17:44:04 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
+----------+--------+-----------+---------+-----------+
|      name|database|description|tableType|isTemporary|
+----------+--------+-----------+---------+-----------+
|     posts| default|       null|  MANAGED|      false|
|     votes| default|       null|  MANAGED|      false|
|posts_temp|    null|       null|TEMPORARY|       true|
+----------+--------+-----------+---------+-----------+


scala> spark.catalog.listColumns("votes").show()
+------------+-----------+---------+--------+-----------+--------+
|        name|description| dataType|nullable|isPartition|isBucket|
+------------+-----------+---------+--------+-----------+--------+
|          id|       null|   bigint|    true|      false|   false|
|      postId|       null|   bigint|    true|      false|   false|
|  voteTypeId|       null|      int|    true|      false|   false|
|creationDate|       null|timestamp|    true|      false|   false|
+------------+-----------+---------+--------+-----------+--------+

scala> spark.catalog.listFunctions.show()
+----------+--------+-----------+--------------------+-----------+
|      name|database|description|           className|isTemporary|
+----------+--------+-----------+--------------------+-----------+
|         !|    null|       null|org.apache.spark....|       true|
|         %|    null|       null|org.apache.spark....|       true|
|         &|    null|       null|org.apache.spark....|       true|
|         *|    null|       null|org.apache.spark....|       true|
|         +|    null|       null|org.apache.spark....|       true|
|         -|    null|       null|org.apache.spark....|       true|
|         /|    null|       null|org.apache.spark....|       true|
|         <|    null|       null|org.apache.spark....|       true|
|        <=|    null|       null|org.apache.spark....|       true|
|       <=>|    null|       null|org.apache.spark....|       true|
|         =|    null|       null|org.apache.spark....|       true|
|        ==|    null|       null|org.apache.spark....|       true|
|         >|    null|       null|org.apache.spark....|       true|
|        >=|    null|       null|org.apache.spark....|       true|
|         ^|    null|       null|org.apache.spark....|       true|
|       abs|    null|       null|org.apache.spark....|       true|
|      acos|    null|       null|org.apache.spark....|       true|
|     acosh|    null|       null|org.apache.spark....|       true|
|add_months|    null|       null|org.apache.spark....|       true|
| aggregate|    null|       null|org.apache.spark....|       true|
+----------+--------+-----------+--------------------+-----------+
only showing top 20 rows





#### SQL 쿼리 실행




scala> val resultDf = sql("select * from posts")
resultDf: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]