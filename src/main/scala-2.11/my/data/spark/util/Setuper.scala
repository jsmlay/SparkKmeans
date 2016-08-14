package my.data.spark.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author jiangshimiao1@jd.com  
  * @version 2016/8/12
  */
object Setuper {
    def getContext(): SparkContext = {
        val conf = new SparkConf().setAppName("Empty Project").setMaster("local")
        val sc = new SparkContext(conf)
        sc
    }
}
