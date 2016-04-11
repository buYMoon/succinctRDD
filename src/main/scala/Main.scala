import edu.berkeley.cs.succinct._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Chris Xie
 * Date: 2016/4/8
 * Description: balala
 */
object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("succinctRDD").setMaster("spark://10.244.48.204:7077")
    val sc = new SparkContext(conf)

    println("Read the test file.")
    // Read text data from file; sc is the SparkContext
    val wikiData = sc.textFile("file:///home/clariion/Downloads/emc_ccoe/tmp/data").map(_.getBytes)

    println("Convert to succinctRDD.")
    // Converts the wikiData RDD to a SuccinctRDD, serializing each record into an
    // array of bytes. We persist the RDD in memory to perform in-memory queries.
    val wikiSuccinctData = wikiData.succinct.persist()

    // Count the number of occurrences of "Berkeley" in the RDD
    val berkeleyOccCount = wikiSuccinctData.count("Berkeley")
    println("# of times Berkeley appears in text = " + berkeleyOccCount)

    // Find all offsets of occurrences of "Berkeley" in the RDD
    val searchOffsets = wikiSuccinctData.search("Berkeley")
    println("First 10 locations in the RDD where Berkeley occurs: ")
    searchOffsets.take(10).foreach(println)

    // Find all occurrences of the regular expression "(berkeley|stanford)\\.edu"
    val regexOccurrences = wikiSuccinctData.regexSearch("(stanford|berkeley)\\.edu").collect()
    println("# of matches for the regular expression (stanford|berkeley)\\.edu = " + regexOccurrences.toString)

    // Extract 10 bytes at offset 5 in the RDD
    val extractedData = wikiSuccinctData.extract(5, 10)
    println("Extracted data = [" + new String(extractedData) + "]")
  }
}
