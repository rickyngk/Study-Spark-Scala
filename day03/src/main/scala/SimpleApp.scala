/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SimpleApp {
	def textSearch(logData: RDD[String]) {
		val errors = logData.filter(line => line.contains("Mr."))
		println(errors.count());
	}

	def main(args: Array[String]) {
		val logFile = "titanic.csv"
		val conf = new SparkConf().setAppName("Simple Application")
		val sc = new SparkContext(conf)
		val logData = sc.textFile(logFile, 2).cache()

		textSearch(logData)
	}
}
