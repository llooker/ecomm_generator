package com.looker.generator

import akka.actor.Actor

object FutureCounter {
  object IDRequest
}


class FutureCounter(t: String) extends Actor {
  var start = 1

  def receive = {
    case FutureCounter.IDRequest => sender() ! getId
  }

  def getId: Int = {
    val retVal = start
    start += 1
    retVal
  }
}
