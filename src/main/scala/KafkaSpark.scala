import org.apache.spark.SparkContext

object KafkaSpark {
  def main(args: Array[String]):Unit =  {

    val sc = new SparkContext("local[*]","KafkaSpark")

    val lines = sc.textFile("C:\\Users\\kmalho4\\Desktop\\DummyText.txt")
    val words = lines.flatMap(_.split(" "))

    val wordsKVRdd = words.map(x => (x,1))
    val count = wordsKVRdd.reduceByKey((x,y) => x + y).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2, x._1)).take(10)
    println("-------------------------------------------------------------------------Output------------------------------------------------------------------------------")
    count.foreach(println)
    println("---------------------------------------------------------------------End--Of---Output------------------------------------------------------------------------")
  }
}