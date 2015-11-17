/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SimpleApp {
	def textSearch(textData: RDD[String]) {
		val errors = textData.filter(line => line.contains("Mr."))
		println(errors.count());
	}

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application")
		val sc = new SparkContext(conf)
		
		//load text file & cache
		val textData = sc.textFile("titanic.csv").cache()

		textSearch(textData)
	}
}
