package io.kewei.lastfm

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by kshang on 19/02/2017.
  */
class SongStatistics extends Serializable{

  val DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

  // Solution A. Create a list of user IDs, along with the number of distinct songs each user has played.
  // Alternative solution:
  // 1. If the file is too big to fit into memory and is stored in HDFS, we can also user Hadoop MapReduce.
  // 2. If the file can fit into memory, we can use a HashMap of Sets while iterating through each row of the file.
  // The HashMap map's userId to the Set of distinct songs the user has listened. Then we count the size of the Set.
  // Time complexity: O(n) where n is the number of lines in file.
  // Space complexity: O(n)
  def countSongsByUser(fileContents: RDD[String]): RDD[(String, Int)] = {
    fileContents.map { line =>
      val row = line.split("\t")
      val userId = row(0)
      // use song as key because some songs do not have track id
      val song = row.last
      (userId, song)
    }.
      // avoid the extra memory overhead associated with returning new Set each time we add values to or merge two Sets
      aggregateByKey(mutable.HashSet.empty[String])(_ + _, _ ++ _).
      mapValues(_.size)
  }

  // Solution B. Create a list of the 100 most popular songs (artist and title), with number of times each was played.
  // Alternative solution:
  // 1. If the file is too big to fit into memory and is stored in HDFS, we can also user Hadoop MapReduce.
  // 2. If the file can fit into memory, we can use a HashMap that maps song to count while iterating through each row
  // of the file. Then we iterate through the map and put the (count, song) tuple in PriorityQueue(max heap), which compares
  // its items by count of the tuple(count, song), finally we take the first 100 items from the PriorityQueue.
  // Time complexity : O(n*ln(n)), where n is the number of line in file
  // Space complexity : O(n)
  def topSongs(fileContents: RDD[String], top: Int = 100): Array[(String, String, Int)] = {
    fileContents.map { line =>
      val row = line.split("\t")
      val song = row.last
      val artist = row(3)
      ((song, artist), 1) // song + artist as key, because different artists may write songs with the same title
    }.
      reduceByKey(_ + _).
      map(_.swap). // swap (song, count) to (count, song)
      sortByKey(ascending = false).
      map {
        case (count, (song, artist)) => (song, artist, count)
      }.take(top)
  }

  // Solution C. Create a list of top 10 longest sessions
  def topSessions(fileContents: RDD[String], top: Int = 10): Array[(String, String, String, mutable.Queue[String])] = {
    fileContents.map { line =>
      // not thread-safe, one instance per thread
      val format = new SimpleDateFormat(DATE_FORMAT)
      val row = line.split("\t")
      val userId = row(0)
      val timestamp = format.parse(row(1))
      val song = row.last
      (userId, (timestamp, song))
    }.
      groupByKey.
      mapValues { iterable =>
        // sort songs by timestamp
        val songVect = iterable.toVector.sortBy {
          case (timestamp, _) => timestamp
        }
        // create sessions for each user
        songVect.foldLeft(List.empty[Session]) {
          case (list, (timestamp, song)) =>
            if (list.isEmpty) List(Session(mutable.Queue(song), timestamp, timestamp))
            else if (list.head.inSession(timestamp)) list.head.addSong(song, timestamp) :: list.tail
            else new Session(mutable.Queue(song), timestamp, timestamp) :: list
        }
      }.
      flatMap {
        // flat all sessions
        case (userId, sessions) =>
          sessions map {
            case session => (session.duration, (userId, session.startTime, session.endTime, session.queue))
          }
      }.
      sortByKey(ascending = false). // sort by duration in descending order
      map {
      case (duration, (userId, startTime, endTime, songs)) =>
        // not thread-safe, one instance per thread
        val format = new SimpleDateFormat(DATE_FORMAT)
        (userId, format.format(startTime), format.format(endTime), songs)
    }.take(top)
  }

}
