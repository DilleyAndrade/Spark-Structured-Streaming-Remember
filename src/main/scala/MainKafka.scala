import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object MainKafka {
  def main(args:Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("StreamingApp")
      .master("local[*]")
      .getOrCreate()


    //READ KAFKA - Lazy
    val df_kafka = spark.readStream //Retorna um DataStreamReader - Interface que lê dados streaming
      .format("kafka") //Especifica a fonte do dado, kafka, json, parquet, csv, delta
      .option("kafka.bootstrap.servers", "localhost:9092") //Endereço do cluster Kafka (Obrigatório)
      .option("subscribe", "my-topic") //Define o tópico - VOLTAR AQUI E ENTENDER MELHOR
      .option("startingOffsets", "earliest") //Quais dados deve ler na pasta. earliest = todos, latest = apenas novos dados
      .option("maxOffsetsPerTrigger", 10000) //Máximo de mensagens por micro-batch para evitar sobrecarga
      .load() //Inicia a leitura do arquivo
    //Vai retornar um Schema com formato: key, value, topic, partition, offset, timestamp

    //Kafka manda o value em binário, vamos converter, para String
    val df_kafka_parsed = df_kafka.select(col("value").cast("string"))

    //READ JSON
    val df = spark.readStream
      .format("json")
      .schema("schema_do_json")
      .load("/data/input/") //Spark monitora o diretorio, cada novo arquivo = 1 novo batch


    //WRITE STREAM
    val query = df_kafka_parsed.writeStream //Cria um DataStreamWriter
      .format("console") //Formato de saída Console = apenas debug, kafka, parquet, delta, jdbc
      .option("path", "/data/output") //Onde os dados serão salvos
      .option("checkpointLocation", "/chk/orders") //Para controle do Kafka saber o que já processou, tipo um log
      .outputMode("append") //Append = novos dados, Update = dados atualizados, Complete = tabela inteira
      .trigger(Trigger.ProcessingTime("10 seconds")) //Frequência dos micro batches - processa a cada 10 segundos
      .start()
      //.trigger(Trigger.AvailableNow()) - Processa tudo e depois para Processamento Contínuo (Baixa latência)
      //.trigger(Trigger.Continuous("1 second")) - Processamento Contínuo (Baixa latência)


    query.awaitTermination()


    spark.stop()

  }
}
