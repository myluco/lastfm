package io.kewei.lastfm

import java.util.Date
import java.util.concurrent.TimeUnit

import scala.collection.mutable

/**
  * Created by kshang on 19/02/2017.
  */
case class Session(queue: mutable.Queue[String], startTime: Date, endTime: Date) extends Serializable {

  private val INTERVAL_IN_MINUTE = 20
  val duration = endTime.getTime - startTime.getTime

  def inSession(newStartTime: Date): Boolean = {
    val duration = newStartTime.getTime - endTime.getTime
    val diffInMinutes = TimeUnit.MILLISECONDS.toMinutes(duration)
    diffInMinutes <= INTERVAL_IN_MINUTE // the new song started within 20 minutes
  }

  def addSong(newSong: String, newStartTime: Date): Session = {
    Session(queue += newSong, startTime, newStartTime)
    //     queue += newSong
    //     endTime = newStartTime
  }

  override def toString: String = {
    s"$startTime --> $endTime"
  }
}
