package org.spiffy.enron

class Samples(val firstStamp: Long, val lastStamp: Long, val interval: Long)
  extends TimeSampled

object Samples {
  def apply(firstStamp: Long, lastStamp: Long, interval: Long) =
    new Samples(firstStamp, lastStamp, interval)
}