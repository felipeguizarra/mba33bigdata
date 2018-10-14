package nasalog

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

/* Esta Operação lista as URL's que mais fizeram requisições ao servidor da NASA
 * É necesário passar o caminho nos argumentos, utilizando Run configurations
 */

object Operacao01_HostsRequisitandoContador {

  //Método principal
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("É necessário inserir a caminho da pasta nos argumentos de execução!")
      System.exit(1)
    }

    // Jogo o caminho do diretório para variável
    val diretorio: String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Operacao01_HostRequisitandoContador")
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
      .option("maxFilesPerTrigger", 1) //Um arquivo de cada vez
      .option("delimiter", " ") //para quando as linhas forem divididas em espaço
      .csv(diretorio)

    // Dataset
    val requisicoes = leituras
      .select("host", "requisicao").as[HostRequisicao]
      .filter(!isnull($"requisicao") && $"requisicao".contains("GET"))

    // Conta agrupando pelos hosts que mais fizeram requisições
    val contagens = requisicoes.groupBy("host")
      .count
      .sort($"count".desc)
      .withColumnRenamed("count", "requisicoes")
      .limit(10)
      
     val path = "/home/felipe/eclipse-workspace/trabalhobigdata/src/resultados/contadorhost"

    // Escreve em arquivo
    contagens
      .write
      .option("delimiter", " ")
      .option("header", "true")
      .csv(path)    

  }

}