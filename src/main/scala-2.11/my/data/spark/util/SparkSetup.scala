package my.data.spark.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark环境初始化
  * @author jsmlay@sina.com
  * @version 2016/8/12
  */
class SparkSetup(appName: String, runType: String) {
    def makeContext(): SparkContext = {
        val conf = new SparkConf().setAppName(appName)
        if (runType != null) {
            conf.setMaster("local")
        }
        new SparkContext(conf)
    }
}
