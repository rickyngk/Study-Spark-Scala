/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SimpleApp {
	val hadoop_uri = "hdfs://172.18.2.108:9000"

	def textSearch(textData: RDD[String]) {
		val cases = textData.filter(line => line.contains("Spark"))
		println(cases.count());
	}

	def wordCount(textData: RDD[String]) {
		val counts = textData.flatMap(line => line.split(" "))
                 		.map(word => (word, 1))
                 		.reduceByKey(_ + _)

        val output = hadoop_uri + "/counts.txt"
		counts.saveAsTextFile(output)
	}

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application")
		val sc = new SparkContext(conf)
		
		//load text file & cache
		val textData = sc.textFile("input.txt").cache()

		textSearch(textData)
		wordCount(textData)
	}
}
