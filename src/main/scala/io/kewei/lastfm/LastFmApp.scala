package io.kewei.lastfm

import org.apache.spark.{SparkConf, SparkContext}

/* LastFmApp.scala */

object LastFmApp {

  private val APP_NAME = "LastFm Application"
  private val USER_SONG_COUNT_OUTPUT = "USER_SONG_COUNT_OUTPUT"
  private val TOP_SONGS_OUTPUT = "TOP_SONGS_OUTPUT"
  private val TOP_SESSIONS_OUTPUT = "TOP_SESSIONS_OUTPUT"

  def main(args: Array[String]) {
    args match {
      case Array(option, fullPath) =>
        println(s"Option is : $option")
        println(s"FilePath is : $fullPath")
        val conf = new SparkConf().setAppName(APP_NAME)
        val sc = new SparkContext(conf)
        val fileContents = sc.textFile(fullPath).cache()
        val service = new SongStatistics()
        option match {
          case "A" =>
            service.countSongsByUser(fileContents).saveAsTextFile(USER_SONG_COUNT_OUTPUT)
          case "B" =>
            sc.parallelize(service.topSongs(fileContents)).saveAsTextFile(TOP_SONGS_OUTPUT)
          case "C" =>
            sc.parallelize(service.topSessions(fileContents)).saveAsTextFile(TOP_SESSIONS_OUTPUT)
          case _ =>
            println(s"Option should be A, B or C : $option")
        }
        sc.stop()

      case _ =>
        println(
          """
            |Please input exact two application arguments Option FilePath,
            |whereas Option should be A, B or C, and FilePath should be the path to the input file.
            |For example:
            |SPARK_DIR/bin/spark-submit \
            |  --class "LastFmApp" \
            |  --master local[4] \
            |  target/scala-2.11/lastfm_2.11-1.0.jar \
            |  A \
            |  /path-to-file/userid-timestamp-artid-artname-traid-traname.tsv
          """.stripMargin)
    }
  }
}