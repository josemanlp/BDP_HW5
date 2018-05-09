import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}


object SentimentAnalysis {
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Get Twitter Token and the filter words
    // By default, use my token and filter word is Trump
    var consumerKey ="F36yD1iItIQhRXiz6OivF2eun"
    var consumerSecret = "cLtRADTh89RNkbtsM69NuFVjL6ZGX6gv4dxXoC07FrLfcabOl3 "
    var accessToken = "282572624-R6DjYVqNTattKyHogcxCF3Gdp1Akl2mqUVI1QZzA"
    var accessTokenSecret = "7IrQN5Sw9AivQagXctZXlhDWji6l9xbk82N5ECzwOQ5Si"

  if (args.length > 3) {
      // get data from your setting
    val  Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    }


    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Set twitter stream
    val sparkConf = new SparkConf().setAppName("Twitter").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None)
      
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

val topCounts30 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(30)).map{case (topic, count) => (count, topic)}.transform(_.sortByKey(false))


// Print popular hashtags
topCounts30.foreachRDD(rdd => {
  val topList = rdd.take(10)
  println("\nPopular topics in the last 30 seconds (%s total):".format(rdd.count()))
  topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
})


    ssc.start()
    ssc.awaitTermination()

  }

}

