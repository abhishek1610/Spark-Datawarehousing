/**
 * Created by root on 05/03/16.
 */
import org.apache.spark.sql.functions._
object MD5gen {

  /*def md5Hash(text1: String): String = {
    val text = text1
    val md5 = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
      "%02x".format(_)
    }.foldLeft("") {
      _ + _

md5
    }*/

    //val sqlfunc = udf(md5Hash)
    //myDF.withColumn("Code", sqlfunc(col("Amt")))


}
