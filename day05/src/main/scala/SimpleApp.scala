/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.linalg.{Vector, Vectors, SparseVector, Matrix, Matrices}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, IndexedRow, IndexedRowMatrix, CoordinateMatrix, MatrixEntry, BlockMatrix}
import org.apache.spark.rdd.RDD

object SimpleApp {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application")
		val sc = new SparkContext(conf)

		//Vectors
		val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
		val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
		val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))

		//Label
		val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
		val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))

		//Local Matrix
		val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
		val sm: Matrix = Matrices.sparse(4, 5, 
		              Array(0, 2, 3, 6, 7, 8), 
		              Array(0, 3, 1, 0, 2, 3, 2, 1), 
		              Array(1.0, 14.0, 6.0, 2.0, 11.0, 16.0, 12.0, 9.0))

		//Row matrix
		val rows: RDD[Vector] = sc.parallelize(Seq(
			Vectors.dense(1.0, 0.0, 5.4, 0.0),
			Vectors.dense(1.0, 0.0, 5.4, 0.0),
			Vectors.dense(1.0, 0.0, 5.4, 0.0)
		))
		val rowMatrix: RowMatrix = new RowMatrix(rows)


		//indexed row matrix
		val indexRows: RDD[IndexedRow] = sc.parallelize(Seq(
			IndexedRow(0, Vectors.dense(1.0, 0.0, 5.4, 0.0)),
			IndexedRow(2, Vectors.dense(1.0, 0.0, 5.4, 0.0))
		))
		val indexedRowMatrix: IndexedRowMatrix = new IndexedRowMatrix(indexRows)

		//Coordinate Matrix 
		val coordinateMatrixEntry: RDD[MatrixEntry] = sc.parallelize(Seq(
			MatrixEntry(0, 0, 1.2),
			MatrixEntry(1, 0, 2.1),
			MatrixEntry(6, 1, 3.7)
		))
		val coordinateMatrix: CoordinateMatrix = new CoordinateMatrix(coordinateMatrixEntry)

		//BlockMatrix
		val blocks = sc.parallelize(Seq(
			((0, 0), Matrices.dense(3, 2, Array(1, 2, 3, 4, 5, 6))), 
         	((1, 0), Matrices.dense(3, 2, Array(7, 8, 9, 10, 11, 12)))
        ))
		val blockMatrix: BlockMatrix = new BlockMatrix(blocks, 3, 2)
	}
}





