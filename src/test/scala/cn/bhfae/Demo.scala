package cn.bhfae

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class Demo {

	@Test
	def demo {
		// 获取sparkSession，并引入隐式转换
		val spark = getSparkSession
		import spark.implicits._

		val arr = Array(("盛世集团", 20), ("命名", 21))
		val rdd = spark.sparkContext.makeRDD(arr)
		//				.map(line => {
		//			val bytes = line._1.getBytes("GBK")
		//			val str2 = new String(bytes, "GBK")
		//			(str2, line._2)
		//		})
		rdd.foreach(println)
		val df = rdd.toDF("name", "age")
		df.show()
		writeToMysql(df)



		//		val ds = spark.createDataset(lines)
		//		val df = spark.read.json(ds)

		// 数据以追加的方式写入到Mysql
		//		writeToMysql(df)
	}

	/**
	  * 根据指定的配置条件获取SparkSession
	  *
	  * @return 返回SparkSession
	  */
	def getSparkSession: SparkSession = {
		val spark: SparkSession = SparkSession
				.builder()
				.appName(s"${this.getClass.getSimpleName}")
				.master("local[5]")
				.config("spark.some.config.option", "some-value")
				.getOrCreate()
		spark
	}

	/**
	  * 根据指定的编码方式读取文件
	  *
	  * @param sc   SparkContext
	  * @param path 读取文件的路径
	  * @return 返回读取到的文件
	  */
	def read(sc: SparkContext, path: String): RDD[String] = {
		sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
				.map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
	}

	/**
	  * 根据指定的条件过滤数据
	  *
	  * @param infomation 需要判断的信息
	  * @return 是否满足条件
	  */
	def filterInfomation(infomation: String): Boolean = {
		// 过滤条件01
		val condition = infomation.contains("2018-12-11")
		// 过滤条件02
		val condition2 = infomation.contains("调用接口")
		// 过滤条件03
		val condition3 = infomation.contains("S010252")
		if (condition && condition2 && condition3) {
			return true
		}
		false
	}

	/**
	  * 提取指定的信息
	  *
	  * @param information 源信息
	  * @return 提取后的信息
	  */
	def extractInformation(information: String): String = {
		val info = "{" + information.split("\\{").last
		info
	}

	/**
	  * 以追加的方式把数据写出到数据库
	  *
	  * @param df 需要存储的时间
	  */
	def writeToMysql(df: DataFrame): Unit = {
		df.write
				.mode("append")
				.format("jdbc")
				.option("url", "jdbc:mysql://127.0.0.1:3306/lottery?useUnicode=true&characterEncoding=gbk")
				.option("user", "root")
				.option("password", "root")
				.option("dbtable", "lot")
				.save()
	}

}
