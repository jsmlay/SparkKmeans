package my.data.spark.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author jsmlay@sina.com
  * @version 2016/8/14
  */
object FileUtil {
    /**
      * Load data from file into RDD
      * FORMAT::  label:Int,feature:Double*
      * Drop if data has col missing
      *
      * @param filePath   Data file path
      * @param featureLen Expected feature vector length
      * @param sc         spark context
      * @return RDD
      */
    def loadVector(filePath: String, featureLen: Int, sc: SparkContext): RDD[Vector[Double]] = {
        sc.textFile(filePath).map(x => x.split(",").map(y => y.trim.toDouble).tail.toSeq.toVector)
            .filter(x => x.length == featureLen)
    }

    def loadLabel(filePath: String, sc: SparkContext): Vector[Int] = {
        sc.textFile(filePath).map(x => x.split(",").map(y => y.trim.toDouble).head.toInt).collect().toVector
    }
}
