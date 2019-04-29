package com.mym.spark.dataFrame

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setAppName("dataFrameTest");
    conf.setMaster("local[2]");
    val sc = new SparkContext(conf);

    val lines = sc.textFile("spark-scala-use/test.log");
    print(lines.flatMap(x => x.split(",")).collect().mkString("#"))
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._;
    //rdd转换为dataFrame，使用关键字toDF
    val df = lines.map{x => val data= x.split(",")
      (data(0),data(1),data(2),data(3))
    }.toDF("id","name","type","introduce");//这里是split后会得到4列数据，然后把这四个数据放到data返回值那几个位置
    df.show(2);
    df.printSchema();
    df.select("name").show();
    df.groupBy("name").count().show();

    //dataFrame 转为 rdd
    val df2Rdd = df.rdd;
    df2Rdd.foreach { x => println(x);}
    //rdd转为dataSet
    val ds = df2Rdd.map { x =>
      (x.get(0).toString, x.get(1).toString, x.get(2).toString, x.get(3).toString)
      }.toDS();
    ds.show();

    //dataset 转为rdd
    val ds2rdd = ds.rdd;
    print(ds2rdd);

    //dataSet转为dataFrame
    val ds2Df = ds.toDF("id","name","type","introByds");
    ds2Df.show();

    //DataFrame 转为 dataSet
    val df2Ds = ds2Df.as("test");
    df2Ds.show();
  }

}
