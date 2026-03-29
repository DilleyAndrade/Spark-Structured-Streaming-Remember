import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object MainCsv {
  def main(args:Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("StreamingApp")
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("name", StringType)
      )
    )

    val df = spark.readStream
      .format("csv") //Formato que será lido
      .schema(schema) //Schema do arquivo
      .option("header", "true") //Se possui cabeçalho
      .option("sep", ",") //Separador do arquivo
      .option("maxFilesPerTrigger", 5) //Quantidade de arquivos que vuo processar a cada batch, no caso de 5 em 5
      .load("/data/input/csv/") //Pasta que será monitorada

  }
}
