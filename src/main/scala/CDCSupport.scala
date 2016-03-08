/**
 * Created by root on 05/03/16.
 */
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object CDCSupport extends Cdc  {

 // import Cdc._

  def computeCdc(newSnapshot: DataFrame, previousSnapshot: DataFrame) :DataFrame = {

    println( s"New snapshot $newSnapshot")
    println( s"Previous snapshot $previousSnapshot")


    import org.apache.spark.sql.Column

   // val deletedRecords = extractDeletedRecords(newSnapshot, previousSnapshot)

    val otherOps =  insertedandupd(newSnapshot, previousSnapshot)
    //val op = otherOps.operation //add to select only active from previous
    val prev = previousSnapshot.select(col("id"),col("payroleid"),col("name"),col("surname"),col("company"),col("dept"),col("ts"))
    val df = otherOps.unionAll(prev)

    val test = scd(df)
    test
  }



  import org.apache.spark.sql.functions.{lead, coalesce}
   def scd (totalsnap: DataFrame) : DataFrame =

  {  val un = totalsnap
    val w = Window.partitionBy("id").orderBy("ts")
    val endts = lead("ts", 1).over(w)

    val un1 = un.withColumn("endts",endts)
    un1
      //if (max(un("ts")).over(w) === un("ts")) max(un("ts")) else


  }




  /*protected def extractDeletedRecords[Key, Value](newSnapshot: RDD[(Key, Value)], previousSnapshot: RDD[(Key, Value)])(implicit kt: ClassTag[Key], vt: ClassTag[Value]): RDD[Cdc[Value]] = {
    newSnapshot
      .rightOuterJoin(previousSnapshot)
      .filter {
      case (_, (None, _)) => true
      case _ => false
    }
      .map { case (_, (_, value)) =>
      deleted(value)
    }  */

}
