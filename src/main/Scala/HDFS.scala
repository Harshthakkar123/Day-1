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
    
    Proble 2nd.....
    
    //load personal data file
    val df=sparkSession.read.format(source = "csv").option("header","true")
      .load(path="hdfs://localhost:9000/app/Employee_personal_details.csv")
   // df.show()

    //load business data file
    val df1=sparkSession.read.format(source = "csv").option("header","true")
      .load(path="hdfs://localhost:9000/app/Employee_Business_Details.csv")
    //df1.show()

    val df2=sparkSession.read.format(source = "csv").option("header","true")
      .load(path="hdfs://localhost:9000/app/Employee_address_details.csv")
    df2.show()

    //create temp view
    df.createTempView("EP")
    df1.createTempView("EB")
    df2.createTempView("EA")



    //count avg,max,min salary
    val s1=sparkSession.sql("SELECT avg(salary) as Average,min(CAST(salary AS int)) as Min,max(CAST(salary AS int)) as Max FROM EP INNER JOIN EB on EP.Emp_ID=EB.Emp_ID where EP.AgeinYrs >30.00 and EP.AgeinYrs < 40.00")
    //s1.show()

    // count Emp
   val s2=sparkSession.sql("SELECT Count(Email),Year_of_Joining FROM EB Group By( Year_of_Joining)")
    //s2.show()

    //group By
    val s3=sparkSession.sql("SELECT Count(Emp_ID),Year_of_Joining FROM EB Group By( Year_of_Joining) Order By (Year_of_Joining)")
   // s3.show()


    //val s4=sparkSession.sql("SELECT Emp_ID,CAST(Salary AS float) AS Current_Salary,LastHiKe,CAST((Salary((Salary*REPLACE(LastHiKE,'%',' '))/100)) AS float ) AS Pre_Salary FROM EB")

    //s4.show()
    val s4=sparkSession.sql("SELECT Emp_Id,Salary AS Current ,LastHike,CAST((Salary - ((Salary*REPLACE(LastHike,'%',''))/100)) AS float) AS previous FROM EB")
    s4.show()

    val s5=sparkSession.sql("SELECT avg(WeightinKgs) FROM EP  INNER JOIN EB on EP.Emp_Id=EB.Emp_Id WHERE DOW_of_Joining='Monday' OR DOW_of_Joining='Wednesday' OR DOW_of_Joining='Friday' ")
    s5.show()
    s5.write.mode(saveMode = "overwrite").csv(path = "hdfs://localhost:9000/app/data/Weight")


    //f
   // val s6=sparkSession.sql("select EA.Emp_ID,EB.First_Name,EB.Last_Name,EA.State,EP.DateofBirth FROM EP JOIN EB on EP.Emp_ID=EB.Emp_ID JOIN EA on EP.Emp_ID=EA.Emp_ID WHERE EA.State='AK' AND EP.DateofBirth>(To_DATE(UNIX_TIMESTAMP('01-01-1980','MM/dd/yyyy')AS TIMESTAMP))")
    //s6.show()
    //last
  // val s7=sparkSession.sql("select COUNT(EA.Emp_ID) ,Max(State) FROM EP INNER JOIN EA ON EP.Emp_ID=EA.Emp_ID Group By (State)")
   // s7.show()
  }



  }


}
