import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by Eddie on 2017/5/31.
  */
class KafkaStreaming extends Serializable {
  PropertyConfigurator.configure("/home/eddie/StreamingTest/log4j-streaming.properties")
  @transient
  val conf = new SparkConf().setAppName("Test")
  @transient
  val sc = new SparkContext(conf)
  @transient
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))

  val topics = Array("log")
  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "disco-0011:9092,disco-0012:9092,disco-0013:9092",
    "value.deserializer" -> classOf[StringDeserializer],
    "key.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "acks" -> "0",
    "group.id" -> "streaming")

  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferBrokers,
    Subscribe[String, String](topics, kafkaParams)
  ).map(record => (record.key().toString, record.value().toString))
  val nodeManagerLog = stream.filter(record => record._1.equals("nodemanager"))
  nodeManagerLog.foreachRDD(rdd =>
    saveAsTextFileAndMerge(
      "hdfs://disco-0011:9000",
      "trace-part",
      rdd)
  )
  nodeManagerLog.print(5)

  def start() {
    ssc.start()
    ssc.awaitTermination()
  }

  def saveAsTextFileAndMerge[T](hdfsServer: String, fileName: String, rdd: RDD[T]) = {
    val sourceFile = hdfsServer + "/trace/each"
    val dstPath = hdfsServer + "/trace/whole"
    merge(sourceFile, dstPath, fileName)
  }

  def merge(srcPath: String, dstPath: String, fileName: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val destinationPath = new Path(dstPath)
    if (!hdfs.exists(destinationPath)) {
      hdfs.mkdirs(destinationPath)
    }
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath + "/" + fileName), true, hadoopConfig, null)
  }


//  val numInputDStream = 8
//  val kafkaDStream = (1 to numInputDStream).map { _ => KafkaUtils.createDirectStream(
//    scc,
//    PreferConsistent,
//    Subscribe[String, String](topics, kafkaParams))}
//
//  kafkaDStream.map(
//    stream => stream.map(
//      record =>
//        (print("key: " + record.key), print("value:" + record.value))))
}

