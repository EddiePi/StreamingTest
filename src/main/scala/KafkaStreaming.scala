import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by Eddie on 2017/5/31.
  */
class KafkaStreaming {
  PropertyConfigurator.configure("/home/eddie/StreamingTest/log4j-streaming.properties")
  val conf = new SparkConf().setAppName("Test")
  val sc = new SparkContext(conf)
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
  )
  val nodeManagerLog = stream.filter(filterNM)
  nodeManagerLog.print(5)

  def start() {
    ssc.start()
    ssc.awaitTermination()
  }

  def filterNM (consumerRecord: ConsumerRecord[String, String]): Boolean = {
    if(consumerRecord.key().toString.equals("nodemanager")) {
      return true
    }
    return false
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
