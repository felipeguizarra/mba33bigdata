package nasalog

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object Operacao04_JanelaDeTempoLog {
  
  //Método principal
  def main(args: Array[String]) :Unit = {
    if(args.length < 1){
      System.err.println("É necessário inserir a caminho da pasta nos argumentos de execução!")
      System.exit(1)
    }    
    
    val diretorio : String = args(0)
    
		Logger.getLogger("org").setLevel(Level.ERROR)
    		
    val spark = SparkSession
      .builder
      .master("local[*]")      
      .appName("Operacao04_JanelaDeTempoLog")
      .getOrCreate()    
      
    import spark.implicits._
    
     //Definindo Esquema
    val esquema = StructType(StructField("host", StringType, true) ::
      StructField("traco1", StringType, true) ::
      StructField("traco2", StringType, true) ::
      StructField("datahora", StringType, true) ::
      StructField("timezone", StringType, true) ::
      StructField("requisicao", StringType, true) ::
      StructField("resposta", IntegerType, true) ::
      StructField("bytes", LongType, true) :: Nil)
        
    val leituras = spark.read
      .schema(esquema)      
      .option("delimiter", " ") //para quando as linhas forem divididas em espaço
    	.csv(diretorio)
    	
    val requisicoes = leituras
      .filter(!isnull($"requisicao"))      
      .select(substring($"datahora", 2, 11) as "data", 
              substring($"datahora", 14, 21) as "hora", 
              $"bytes" as "bytes")
    
     val reqwindow = requisicoes
    	.select( unix_timestamp(
    	    format_string("%s %s", $"data", $"hora"), "dd/MMM/yyyy HH:mm:ss" // MMM quando o mês é apenas as iniciais por extenso 
    	    ).cast("timestamp") as "tempo", 
    	    $"bytes" as "bytesResposta").as[TimestampResposta]    	
          	
    val somatorio = reqwindow      
      .groupBy(        
    		window($"tempo", "1 day")
    	)    	
    	.agg(sum("bytesResposta").as("somatorio_bytesresposta"))
    	.select($"window.start" as "inicio", $"window.end" as "fim", $"somatorio_bytesresposta")
    	.sort($"somatorio_bytesresposta".desc)
    	.limit(30)

    val path = "/home/felipe/eclipse-workspace/trabalhobigdata/src/resultados/byteswindow"
    
    // Escreve em arquivo
    somatorio
      .write
      .option("delimiter", " ")
      .option("header", "true")
      .csv(path)
        	
   // WriteStream está imprimindo vazio
   /* 	
    val query = somatorio.writeStream
      .outputMode(Complete)
      .format("console")      
      .trigger(Trigger.ProcessingTime(5.seconds))
//      .option("path", "/home/felipe/eclipse-workspace/trabalhobigdata/src/caminho/bytes.csv")
//      .option("checkpointLocation", "/home/felipe/eclipse-workspace/trabalhobigdata/src/checkpoint/")
      .start()

      query.awaitTermination()   
*/
    
  }
  
  
}