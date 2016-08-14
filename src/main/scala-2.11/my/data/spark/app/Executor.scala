package my.data.spark.app

import my.data.spark.algorithm.Kmeans
import my.data.spark.util.{FileUtil, Setuper}

/**
  * Driver of the SPARK DATA-MINING program
  *
  * @author jiangshimiao1@jd.com
  * @version 2016/8/12
  */
object Executor {
    def main(args: Array[String]): Unit = {
        val sc = Setuper.getContext()
        val input = getClass.getResource("/iris.csv").toString
        val corpus = FileUtil.loadVector(input, 4, sc)
        val labels = FileUtil.loadLabel(input,4, sc)
        val centers = Kmeans.initCenters(corpus, 3, sc)
        val res = Kmeans.kmeans(corpus, centers, 3, 0.0005, 20)
        val labelMap = sc.makeRDD(res).zip(sc.makeRDD(labels)).map(x => x -> 1).reduceByKey((x,y) => x+y).sortBy(x => x
            ._2,ascending = false).take(3).map(x => x._1._1 -> x._1._2).toMap//.foreach(x => println(x))
        val precision = res.zip(labels).map(x => if(labelMap.getOrElse(x._1, 0)==x._2) 1 else 0).sum.toDouble / res
            .length.toDouble
        println(precision)
    }
}