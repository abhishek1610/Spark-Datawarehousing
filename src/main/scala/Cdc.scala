import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


trait Cdc {
  //private case class CdcImpl(record: DataFrame, operation: String)

  def insertedandupd (inp: DataFrame, snp:DataFrame): DataFrame =

  {  inp.show()
    val insert_recds = inp.join(
      snp,
      inp("id") <=> snp("id") ,
      "left"
    ).select(inp("id").alias("id"),inp("payroleid").alias("payroleid"),inp("name").alias("name"),inp("surname").alias("surname"),
        inp("age").alias("age"),inp("company").alias("company"),inp("dept").alias("dept"),inp("md5col").alias("md5col"),
        inp("ts").alias("ts"),snp("id").alias("id1"))
    .where(col("id1") isNull ).select(col("id"),col("payroleid"),col("name"),col("ts"))

    insert_recds.show();
    val upd_recds = inp.join(
      snp,
      inp("id") <=> snp("id") ,
      "left"
    ).select(inp("id").alias("id"),inp("payroleid").alias("payroleid"),inp("name").alias("name"),inp("surname").alias("surname"),
        inp("age").alias("age"),inp("company").alias("company"),inp("dept").alias("dept"),inp("md5col").alias("md5col"),snp("id").alias("id1"),
        snp("md5col").alias("md5col1"), inp("ts").alias("ts"))
      .where(col("id1") isNotNull ).where(col("md5col") === col("md5col1")).select("id","payroleid","name","ts")

    val final_rcds = insert_recds.unionAll(upd_recds)
    final_rcds

  }
  //def deleted(record: DataFrame): Cdc = new CdcImpl(record, "D")
  //def updated(record: DataFrame): Cdc = new CdcImpl(record, "U")
 // def noop[T](record: T): Cdc[T] = new CdcImpl(record, "")

 // def isNoop(cdc: Cdc[_]): Boolean = cdc.operation == ""

  //def printAsStringArray[T](cdc: Cdc[T])(implicit recordExtractor: T => Array[String]): Array[String] = recordExtractor(cdc.record) ++ Array(cdc.operation)
}
