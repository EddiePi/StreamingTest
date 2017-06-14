/**
  * Created by eddie on 6/1/17.
  */
object Simple {
  def main(args: Array[String]) {
    val streaming = new KafkaStreaming
    streaming.start()
  }
}