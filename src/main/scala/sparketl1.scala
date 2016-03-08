import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.rdd.RDD
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar

import org.apache.spark.sql.functions._

// this is used to implicitly convert an RDD to a DataFrame.

import MD5gen._

import CDCSupport._

case class Person (id: String, depid: String,name: String,sur: String,smtg: String, dept: String,grade: String,md5col :String  )
case class input1( id:String, payroleid:Int,name:String, surname:String,age:Int ,company:String,dept:String,md5col :String, ts :Int )
case class table1( id:String, payroleid:Int,name:String, surname:String,age:Int ,company:String,dept:String,startdt:String, enddate:String )


object sparketl1 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local[2]"))
    val sqlContext1 = new org.apache.spark.sql.hive.HiveContext(sc)

    //val sqlContext1 = new org.apache.spark.sql.SQLContext(sc)


    import sqlContext1.implicits._

    //val threshold = args(1).toInt

    // split each document into words
    val inp = sc.textFile(args(0))

    import MD5gen._
    val snpsht = sc.textFile(args(1))
    val md5 = udf(md5Hash _)
    val concatfnc = udf(myConcatFunc)

    val inp1 = inp.map(x => x.split(",")).map { p => (p(0), p(1), p(2), p(3), p(4), p(5), p(6),p(7)) }
    //.map{x =>(x._1, x._2, x._3, x._4, x._5, x._6, x._7)  }//.toDF()
    val inp1df = inp1.map { x => input1(x._1, x._2.toInt, x._3, x._4, x._5.toInt, x._6, x._7,"test",x._8.toInt) }.toDF()

    val cols = array("name", "surname","age" ,"company","dept")
    val sep = lit("-")
    val modinpldf = inp1df.withColumn("MD5",md5(concatfnc(cols)))
    //val modinpldf = inp1df.withColumn("MD5", sqlfunc(col("name"),col("surname")))
    val snpsht1 = snpsht.map(x => x.split(",")).map(p => (p(0), p(1), p(2), p(3), p(4), p(5), p(6),p(7)))
    val snpdf = snpsht1.map { x => input1(x._1, x._2.toInt, x._3, x._4, x._5.toInt, x._6, x._7, "test",x._8.toInt) }.toDF()
    val modsnpdf = snpdf.withColumn("MD5", md5(concatfnc(cols)))
    //new

    val input_tab = inp1df.registerTempTable("input_tab")

    val snptable = snpdf.registerTempTable("snapshot_tab")
    val act = computeCdc(modinpldf,modsnpdf)
      //computeCdc(modinpldf,snpdf)
    act.show();

    //new

    val new_rcds = sqlContext1.sql("select input_tab.id from input_tab left outer join snapshot_tab on input_tab.id = snapshot_tab.id where snapshot_tab.id is null")

    //chngd

    val chngd_rcds = sqlContext1.sql("select input_tab.id from input_tab inner join snapshot_tab on input_tab.id = snapshot_tab.id where snapshot_tab.md5col <> input_tab.md5col ")


    modinpldf.map(t => t(8)).collect().foreach(println)


}



  val myConcatFunc = (xs: Seq[Any]) =>
    xs.filter(_ != null).mkString( " ")






}