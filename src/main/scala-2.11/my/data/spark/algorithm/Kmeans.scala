package my.data.spark.algorithm

import my.data.spark.util.VectorUtil._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
  * @author jiangshimiao1@jd.com  
  * @version 2016/8/12
  *
  * K-means Algorithm
  */
object Kmeans {
    /**
      * Init needed cluster centers
      *
      * @param data Origin RDD data
      * @param k    k cluster
      * @return Array[ Vector[Double] ]
      */
    def initCenters(data: RDD[Vector[Double]], k: Int, sc: SparkContext): Array[Vector[Double]] = {
        val withReplace = false
        data.takeSample(withReplace, k, Random.nextLong())
    }

    /**
      *
      * @param data Origin RDD data
      * @return
      */
    def reallocateCenter(data: RDD[(Int, Vector[Double])]): Array[Vector[Double]] = {
        data.map(x => x._1 -> (x._2, 1)).reduceByKey((x, y) => (vadd(x._1, y._1), x._2 + y._2)).map(x =>
            vdiv(x._2._1, x._2._2)).collect()
    }

    def assign(v: Vector[Double], centers: Array[Vector[Double]]): Int = {
        centers.map(x => edist(v, x)).zip(centers.indices).sortWith((x, y) => x._1 < y._1).head._2
    }

    def assignCluster(corpus: RDD[Vector[Double]], centers: Array[Vector[Double]]): RDD[(Int, Vector[Double])] = {
        corpus.map(x => (assign(x, centers), x))
    }

    def kmeans(corpus: RDD[Vector[Double]], centerData: Array[Vector[Double]], k: Int, threshold: Double, maxIterCount:
    Int): Vector[Int] = {
        var lastDist = Double.MaxValue
        var sumDis = Double.MaxValue
        var diff = 0d
        var iterCount = 0
        var centers = centerData
        var newCenters: Array[Vector[Double]] = null
        do {
            newCenters = reallocateCenter(assignCluster(corpus, centers))
            sumDis = newCenters.zip(centers).map(x => edist(x._1, x._2)).sum
            centers = newCenters
            diff = lastDist - sumDis
            lastDist = sumDis
            iterCount += 1
            println("Iteration:" + iterCount + "  threshold:" + threshold + "  Value:" + diff)
//            println("centers data :\n------------")
//            centers.foreach(x => println(x.mkString(",")))
//            println("------------")
        } while (diff > threshold && iterCount < maxIterCount)
        assignCluster(corpus, centers).map(x => x._1).collect().toVector
    }
}
