package flink.job

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.api.common.serialization.SimpleStringSchema

import java.util.Properties

object NthPrimeJob {

  def nthPrime(n: Int): Int = {
    var count = 0
    var num = 1
    while (count < n) {
      num += 1
      var isPrime = true
      var i = 2
      while (i * i <= num && isPrime) {
        if (num % i == 0) isPrime = false
        i += 1
      }
      if (isPrime) count += 1
    }
    num
  }

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // env.setParallelism(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "kafka:9092")
    props.setProperty("group.id", "flink-group")

    val consumer = new FlinkKafkaConsumer[String](
      "input",
      new SimpleStringSchema(),
      props
    )

    val producer = new FlinkKafkaProducer[String](
      "output",
      new SimpleStringSchema(),
      props
    )

    env
      .addSource(consumer)
      .map { msg =>
        val Array(nStr, ts) = msg.split("\\|")
        val n = nStr.toInt
        val prime = nthPrime(n)
        s"$n,$prime,$ts"
      }
      .addSink(producer)

    env.execute("NthPrimeJob")
  }
}
