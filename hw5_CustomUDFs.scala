import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object CustomUDFs {

  def ipToInt(ipString: String): Long = {
    // 192.168.0.1 > 23458723
    val ipGroups = ipString
      .split("\\.")
      .map(x => x.toLong)

    assert(ipGroups.length == 4)

    (ipGroups(0) << 24) + (ipGroups(1) << 16) + (ipGroups(2) << 8) + ipGroups(3)
  }

  val ipToIntUDF: UserDefinedFunction = udf(ipToInt _)

  def IntToIp(ipNum: Long): String = {
    // 23458723 > 192.168.0.1
    ((ipNum & 255 << 24) >> 24).toString + "." + ((ipNum & 255 << 16) >> 16).toString + "." + ((ipNum & 255 << 8) >> 8).toString + "." + ((ipNum & 255 << 0) >> 0).toString
  }

  val IntToIpUDF: UserDefinedFunction = udf(IntToIp _)

}