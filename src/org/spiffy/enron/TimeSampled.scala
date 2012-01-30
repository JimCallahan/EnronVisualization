package org.spiffy.enron

trait TimeSampled {
  /** The earliest time stamp (UTC milliseconds). */ 
  val firstStamp: Long
  
  /** The latest time stamp (UTC milliseconds). */ 
  val lastStamp: Long
  
  /** The sampling interval (in milliseconds). */ 
  val interval: Long

  /** Whether the given time stamp is within the valid range. */
  def inRange(stamp: Long) = (firstStamp <= stamp) && (stamp <= lastStamp)
  
  /** The number of milliseconds since the start of the containing sample interval. */
  def relative(stamp: Long) = (stamp - firstStamp) % interval 

  /** The time stamp (UTC milliseconds) of the start of the containing sample interval. */
  def intervalStart(stamp: Long) = stamp - relative(stamp)

  /** The index of the containing sampling interval. */
  def intervalIndex(stamp: Long) = ((stamp - firstStamp) / interval).toInt
  
  /** The number of time entries. */
  def size = intervalIndex(lastStamp) 
}