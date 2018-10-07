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

object Operacao03_SomatorioBytesResposta {
  
   //Classe principal
  def main(args: Array[String]) :Unit = {
    if(args.length < 1){
      System.err.println("É necessário inserir a caminho da pasta nos argumentos de execução!")
      System.exit(1)
    }
    
     // Jogo o caminho do diretório para uma variável
    val diretorio : String = args(0)
    
		Logger.getLogger("org").setLevel(Level.ERROR)
    		
    val spark = SparkSession
      .builder
      .master("local[*]")      
      .appName("Operacao02_UrlMaisAcessadaContador")
      .getOrCreate()    
      
    import spark.implicits._
    
     //Definindo Esquema
    val esquema = StructType(StructField("host", StringType, true) ::
        StructField("timestamp", StringType, true) ::
        StructField("timezone", StringType, true) ::        
        StructField("requisicao", StringType, true) ::
//        StructField("pagina", StringType, true) ::
//        StructField("tipo", StringType, true) ::
        StructField("resposta", IntegerType, true) ::
        StructField("bytesresposta", LongType, true) :: 
        Nil)
        
     val leituras = spark.readStream
      .schema(esquema)
      .option("delimiter", "\t") //para quando as linhas forem divididas em espaço
    	.csv(diretorio)
    	
    val logInfo = leituras
      .filter(!isnull($"host"))
      .select("host", "resposta","bytesresposta").as[Log]
        
    
    // Agrupa por HOST	
    val somatorio = logInfo
    	.agg(sum("bytesresposta").as("somatorio_bytesresposta"))
    	.sort($"somatorio_bytesresposta".desc)
    
    //Imprime a contagem no console	
    val query = somatorio.writeStream
      .outputMode(Complete)
      .format("console")
      .start
      
	  query.awaitTermination()
	  
	  //Tentei usar parquet mas, não rolou
    //    logInfo.write.mode("overwrite").parquet("log.parquet")
    //    
    //    val logParquet = spark.read.parquet("log.parquet")
    //    
    //    logParquet.createOrReplaceTempView("arquivoParquet")
    
    //val logBytesPorHost = spark.sql("SELECT host, sum(bytesresposta) FROM arquivoParquet GROUP BY host")
    
  }
  
}