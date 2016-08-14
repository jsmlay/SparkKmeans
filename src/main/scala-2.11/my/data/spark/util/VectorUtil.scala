package my.data.spark.util

/**
  * 向量基本操作：加、欧式距离、向量除整数
  * @author jsmlay@sina.com
  * @version 2016/8/14
  */
object VectorUtil {
    def vadd(v1: Vector[Double], v2: Vector[Double]): Vector[Double] = {
        v1.zip(v2).map(x => x._1 + x._2)
    }

    def vdiv(v: Vector[Double], n: Int): Vector[Double] = {
        v.map(x => x / n)
    }

    def edist(v1: Vector[Double], v2: Vector[Double]): Double = {
        v1.zip(v2).map(x => Math.pow(x._1 - x._2, 2)).sum
    }
}
