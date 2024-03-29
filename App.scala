import scala.io._
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
import scala.util.Random

case class Stock(date: Long, open: Double, high: Double, low: Double, close: Double)
case class Point(x: Double, y: Double)


object App {

  def dateToInt(date: String): Long = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    formatter.parseMillis(date)
  }

  def kMeans(data: RDD[((String, Long), (Double, Int))], ctrs: RDD[(Int, (Long, Double))]): (RDD[((String, Long), (Double, Int))], RDD[(Int, (Long, Double))]) = {
    val ret = data.cartesian(ctrs).map(e => {
      val a = Point(e._1._1._2.toDouble, e._1._2._1.toDouble)
      val b = Point(e._2._2._1.toDouble, e._2._2._2.toDouble)
      val d = ((b.x - a.x) * (b.x - a.x)) + ((b.y - a.y) * (b.y - a.y))
      ((e._1._1._1, e._1._1._2), (d, e._2._1))
    }).groupByKey().mapValues(e => e.minBy(_._1))
    
    val tmp = ret.groupBy(_._2._2)
      .map(e => (e._1, e._2.aggregate((0L, 0d))((u, e) => ((u._1 + e._1._2).toLong, u._2 + e._2._1), (a, b) => (a._1 + b._1, a._2 + b._2))))
      .map(e => (e._1, (((1 / e._1.toDouble) * e._2._1).toLong, (1 / e._1.toDouble) * e._2._2)))
      .cartesian(ctrs).map(e => if (e._1._1 == e._2._1) e._1 else e._2)
    
    (ret, tmp)
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils/")

    val MAX_PRICE = 10000d

    val conf = new SparkConf().setAppName("App").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val parseLine = (line: String) => {
      val e = line.split(",")
      Stock(dateToInt(e(0)), e(1).toDouble, e(2).toDouble, e(3).toDouble, e(4).toDouble)
    }

    val aapl = sc.textFile("AAPL.txt").map(parseLine).keyBy(s => ("AAPL", s.date))
    val goog = sc.textFile("GOOG.txt").map(parseLine).keyBy(s => ("GOOG", s.date))
    val msft = sc.textFile("MSFT.txt").map(parseLine).keyBy(s => ("MSFT", s.date))
    val tsla = sc.textFile("TSLA.txt").map(parseLine).keyBy(s => ("TSLA", s.date))

    val aaplMvmt = aapl.mapValues(s => (s.close - s.open, 0)).sortBy(_._1._2)
    val googMvmt = goog.mapValues(s => (s.close - s.open, 0)).sortBy(_._1._2)
    val msftMvmt = msft.mapValues(s => (s.close - s.open, 0)).sortBy(_._1._2)
    val tslaMvmt = tsla.mapValues(s => (s.close - s.open, 0)).sortBy(_._1._2)

    var data = aaplMvmt.union(googMvmt).union(msftMvmt).union(tslaMvmt).sortBy(_._1._2)

    val k = 3
    var i = 0

    var ctrs = sc.parallelize(new Array[(String, Double)](k).map(_ => {
      i += 1
      val year = Random.nextInt(2022 - 1970) + 1970
      val day = Random.nextInt(365) + 1
      (i, (((year * 365) + day).toLong, MAX_PRICE * Random.nextDouble()))
    }))

    val tmp = kMeans(data, ctrs)

    data = tmp._1
    ctrs = tmp._2

    data.saveAsTextFile("data.txt")
    ctrs.saveAsTextFile("centers.txt")
  }
}
