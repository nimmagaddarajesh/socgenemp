package empusecase

import com.socgen.empusecase._
import org.apache.spark.sql.DataFrame
import org.junit._
import org.junit.Assert._
import org.junit.Before._
class TestEmployee {

  private var queryOneRes:DataFrame = null
  private var queryTwoRes:DataFrame= null
  val spark = SparkMain.createSparkObj()
  import spark.implicits._

  @Before def testIntialize() {
    val datasetPath = "E:/mywork/eclipseWS/usecase/src/main/resources/"

    queryOneRes = spark.read.option("header", "true").csv(datasetPath + "/queryOneRes/")
    queryTwoRes = spark.read.option("header", "true").csv(datasetPath + "/queryTwoRes/")

  }
  @Test
  def validateQueryOneResults() {
   val listOne= queryOneRes.filter(($"Age" < 30 && $"ctc" < 30000)).collect().map(_(0)).toList
       assertTrue(listOne.isEmpty)
  }
  @Test
  def validateQueryTwo() {
    val listTwo:Int = queryTwoRes.select($"totalCount")
    .collect().map(_(0)).toList.head.toString.toInt
    assertEquals(14,listTwo)
  }
  
  @After def terminateSparkSession(){
    spark.stop()
  }
}