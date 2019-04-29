package com.mym.spark.TFD

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 本地词语TF-IDF
  */
object TFIDFTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setMaster("local[2]");
    conf.setAppName("tf-idf-test-1");
    val sc = new SparkContext(conf);

    val sqlcontext = new SQLContext(sc);
    import sqlcontext.implicits._;
    //认为三个文档，并且组成一个语料库
    val sData = sqlcontext.createDataFrame(Seq(
      (0,"hi i am mym"),
      (1,"i wish java could use case classes"),
      (1,"Logistic regression models are neat")
    )).toDF("label", "sentence");

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
    val wordData = tokenizer.transform(sData);
    wordData.show();

    //得到TF
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20);//此值要比word数量多
    val featurizedData = hashingTF.transform(wordData);

    //得到IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
    //一般模型计算完毕我们会保存到hdfs中，为了以后数据的加载模型计算
    val idfModel = idf.fit(featurizedData);
    val rescaledData = idfModel.transform(featurizedData);
    rescaledData.select("features","label").take(3).foreach(println);


  }

}
