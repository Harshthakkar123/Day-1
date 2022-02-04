import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

object HDFS {
  def main(args:Array[String]): Unit={

    val sparkSession = SparkSession.builder()
      .appName(name = "this is first scala")
      .master(master = "local")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventing.dir", "file:///home/sterlite/Spark/spark-events")
      .config("spark.history.fs.logDirectory", "file:///home/sterlite/Spark/spark-events")
      .getOrCreate()

      //read hdfs file
    val df=sparkSession.read.format(source = "csv").option("header","true")
      .load(path="hdfs://localhost:9000/app/employee_address_details.csv")

    df.show(200)
    df.write.mode(saveMode = "overwrite").csv(path = "hdfs://localhost:9000/app/data/All")


  //filter condition and showing data
     val h1=df.filter(df("Region")==="South")
    //h1.show()
     val h2=df.filter(df("Region")==="Northeast")
   // h2.show()
     val h3=df.filter(df("Region")==="Midwest")
    //h3.show()
     val h4=df.filter(df("Region")==="West")
   // h4.show()
    val h5=df.filter(df("Region")==="East")
    //h5.show()

    //storing region data
    h1.write.mode(saveMode = "overwrite").csv(path = "hdfs://localhost:9000/app/Region=South/South.csv")
    h2.write.mode(saveMode = "overwrite").csv(path = "hdfs://localhost:9000/app/Region=Northeast/Northeast")
    h3.write.mode(saveMode = "overwrite").csv(path = "hdfs://localhost:9000/app/Region=Midwest/Midwest")
    h4.write.mode(saveMode = "overwrite").csv(path = "hdfs://localhost:9000/app/Region=West/West")
    h5.write.mode(saveMode = "overwrite").csv(path = "hdfs://localhost:9000/app/Region=East/East")

    //Storing Each data region vise in single line without using diffrent dataframe
  df.write.partitionBy("Region").csv(path = "hdfs://localhost:9000/app/NewData1")


  }


}
