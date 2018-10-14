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
  
   //Método principal
  def main(args: Array[String]) :Unit = {
    if(args.length < 1){
      System.err.println("É necessário inserir a caminho da pasta nos argumentos de execução!")
      System.exit(1)
    }
    
     // Jogo o caminho do diretório para uma variável
    val diretorioJuly : String = args(0)
    val diretorioAug : String = args(1)
    
		Logger.getLogger("org").setLevel(Level.ERROR)
    		
    val spark = SparkSession
      .builder
      .master("local[*]")      
      .appName("Operacao03_SomatorioBytesResposta")
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
        
     val leiturasJuly = spark.read
      .schema(esquema)
      .option("delimiter", " ") //para quando as linhas forem divididas em espaço
    	.csv(diretorioJuly)
    	
    val leiturasAug = spark.read
      .schema(esquema)
      .option("delimiter", " ") //para quando as linhas forem divididas em espaço
    	.csv(diretorioAug)
    	
    val logInfoJuly = leiturasJuly
      .filter(!isnull($"host"))
      .select($"host", $"resposta", $"bytes" as "bytesResposta").as[Log]
    
    val logInfoAug = leiturasAug
      .filter(!isnull($"host"))
      .select($"host", $"resposta", $"bytes" as "bytesResposta").as[Log]
    
    // Soma os bytes por diretório
    val somatorioJuly = logInfoJuly
    	.agg(sum("bytesresposta").as("somatorio_bytesresposta_julho95"))
    	
    	
    // Soma os bytes por diretório2
    val somatorioAug = logInfoAug
    	.agg(sum("bytesresposta").as("somatorio_bytesresposta_agosto95"))    	
    	
    somatorioJuly.show
    somatorioAug.show
    
    val pathJul = "/home/felipe/eclipse-workspace/trabalhobigdata/src/resultados/somatorioJuly"
    val pathAug = "/home/felipe/eclipse-workspace/trabalhobigdata/src/resultados/somatorioAug"
    
    somatorioJuly
      .write
      .option("delimiter", " ")
      .option("header", "true")
      .csv(pathJul)
      
    somatorioAug
      .write
      .option("delimiter", " ")
      .option("header", "true")
      .csv(pathAug)
      
    
    //Imprime a contagem no console	
    /*val query = somatorioJuly.writeStream
      .outputMode(Complete)
      .format("console")
      .start
      
	  query.awaitTermination()*/
	  
	  //Tentei usar parquet mas, não rolou
    //    logInfo.write.mode("overwrite").parquet("log.parquet")
    //    
    //    val logParquet = spark.read.parquet("log.parquet")
    //    
    //    logParquet.createOrReplaceTempView("arquivoParquet")
    
    //val logBytesPorHost = spark.sql("SELECT host, sum(bytesresposta) FROM arquivoParquet GROUP BY host")
    
  }
  
}