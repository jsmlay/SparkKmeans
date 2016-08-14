package my.data.spark.app

import my.data.spark.algorithm.Kmeans
import my.data.spark.util.{FileUtil, SparkSetup}

/**
  * Driver of the SPARK DATA-MINING program
  *
  * @author jsmlay@sina.com
  * @version 2016/8/12
  */
object Executor {
    def main(args: Array[String]): Unit = {
        var dataPath = "/iris.csv"
        var K = 3
        var threshold = 0.005
        var maxIter = 20
        var featureLen = 4
        var runType = "local"

        if(args.length == 0){
            // local IDE model
        } else if(args.length == 5){
            dataPath = args(0)
            K = args(1).toInt
            threshold = args(2).toDouble
            maxIter = args(3).toInt
            featureLen = args(4).toInt
            runType = null
        } else {
            println("Usage : Executor dataPath, K, threshold, maxInterTimes, featureLength\n" +
                "   -dataPath : Input file path with FORMAT goldenlabel:Int,feature:Double*\n" +
                "   -K        : K clusters \n" +
                "   -threshold: Minimum change of Sum of cluster's distance \n" +
                "   -maxInter : Maximun of iterations \n" +
                "   -feaLen   : Length of features to filter records that missing feature data \n")
        }

        val sc = new SparkSetup("Spark K-means", runType).makeContext()
        val input = getClass.getResource(dataPath).toString
        val corpus = FileUtil.loadVector(input, featureLen, sc)
        val labels = FileUtil.loadLabel(input,sc)
        val centers = Kmeans.initCenters(corpus, K, sc)
        val res = Kmeans.kmeans(corpus, centers, K, threshold, maxIter)
        val labelMap = sc.makeRDD(res).zip(sc.makeRDD(labels)).map(x => x -> 1).reduceByKey((x,y) => x+y).sortBy(x => x
            ._2,ascending = false).take(K).map(x => x._1._1 -> x._1._2).toMap
        val precision = res.zip(labels).map(x => if(labelMap.getOrElse(x._1, 0)==x._2) 1 else 0).sum.toDouble / res
            .length.toDouble
        println(precision)
    }
}