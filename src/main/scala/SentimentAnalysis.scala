import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}


object SentimentAnalysis {
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)


    // Get Twitter Token and the filter words
    // By default, use my token and filter word is Trump
    var consumerKey ="PD7ERIVkqJ3xsg1V0rwmu43ok"
    var consumerSecret = "RyCgiLKqP8kQjcwxJ8h9EQFzLGm3dL5n2eCTN9YpQ2RRYG3cd7"
    var accessToken = "931749427211128832-UJP8jUVAEieK0fP9mmHn5yuD4DiGi8M"
    var accessTokenSecret = "TAuHxwpL6FghEom8IDUtQTQPUeHik6nxhjmhzvyGlfGUk"
    var filters = Seq("Trump")

  if (args.length > 3) {
      // get data from your setting
    val  Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
      filters = args.takeRight(args.length - 4)
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
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val tags = stream.flatMap { status => status.getHashtagEntities.map(_.getText)}

    // save tages to files
    tags.countByValue()
      .foreachRDD { rdd =>val now = org.joda.time.DateTime.now()
        val savepath = now.toString("yyyy_MM_dd__hh__mm")
        rdd.sortBy(_._2).map(x => (x, now)).saveAsTextFile(s"output/tags/$savepath")
      }


    // save whole live twitter in trump data
    val tweets = stream.filter {t =>
      val tags = t.getText.split(" ").filter(_.startsWith(filters(0))).map(_.toLowerCase)
      tags.exists { x => true }
    }


    val data = tweets.map { status =>
      val sentiment = SentimentAnalysisUtils.detectSentiment(status.getText)

      val tagss = status.getHashtagEntities.map(_.getText.toLowerCase)
        println(status.getText,  tagss.toString(),sentiment.toString)
      println("=======================================================")
      (status.getText,  tagss.toString(),sentiment.toString)


    }

    // save path

    data.saveAsTextFiles("output/twitter_and_rating/t")

    ssc.start()
    ssc.awaitTermination()

  }

}

