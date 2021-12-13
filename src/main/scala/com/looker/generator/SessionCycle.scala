package com.looker.generator

import akka.actor.{ActorRef, ActorLogging, Props, Actor}
import akka.event.LoggingReceive
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request

import scala.annotation.tailrec
import scala.concurrent.Promise

object SessionCycle {
  case object Done
  case class Init(actorRef: ActorRef)
  case class CancelMessage(killCount: Int)

  trait SessionMessage

  case class SessionTerminated(sequenceNumber: Int) extends SessionMessage

  object CycleTail {
    def props = Props(new CycleTail)
  }

  class CycleTail extends Actor with ActorLogging {

    var sourceActor: Option[ActorRef] = None

    var killCount = 0
    var count : Option[Int] = None

    def startNextSession(session: SessionRequest) = {
      sourceActor.get ! session
    }

    def terminateSession(): Unit = {
      killCount += 1
      if (count.nonEmpty && killCount == count.get) {
        sourceActor.get ! CancelMessage(killCount)
      }
    }

    def receive: Receive = LoggingReceive {
      case Done => {}
      case CancelMessage(c) => count = Some(c)
      case msg @ Init(actorRef) => {
        sourceActor = Some(actorRef)

        actorRef ! Init(self)
      }
      case session    : SessionRequest    => startNextSession(session)
      case terminated : SessionTerminated => terminateSession()
    }
  }

  object CycleHead {
    def props(promise: Promise[Unit]) = Props(new CycleHead(promise))
  }

  class CycleHead(promise: Promise[Unit]) extends ActorPublisher[SessionRequest] with ActorLogging {

    val MaxBufferSize = 10000000
    var buf = Vector.empty[SessionRequest]
    var sendCount = 0

    var tail : Option[ActorRef] = None

    def receive: Receive = LoggingReceive {
      // for debugging
      case message : SessionRequest if buf.size == MaxBufferSize => { log.error(s"SourceActor: buffer overflow") }
      case message : SessionRequest => {
        if (tail.isEmpty || tail.get != sender) {
          sendCount += 1
        }
        if (buf.isEmpty && totalDemand > 0) {
          onNext(message)
        } else {
          deliverBuf(Some(message))
        }
      }
      case Done => tail.get ! CancelMessage(sendCount)
      case Request(_) => deliverBuf()
      case CancelMessage(killCount) if killCount == sendCount => terminate(killCount)
      // for debugging
      case CancelMessage(killCount) => println(sendCount + "  - " + killCount)
      case Init(actorRef) => tail = Some(actorRef)
    }

    def terminate(killCount: Int) = {
      context.stop(self)
      promise.success(Unit)
    }

    @tailrec final def deliverBuf(message : Option[SessionRequest] = None): Unit = {
      if (message.nonEmpty) { buf :+= message.get }
      // don't send anything if nothing can be sent
      if (totalDemand <= 0) { return }

      val toSend = Math.min(totalDemand, Int.MaxValue)

      val (use, keep) = buf.splitAt(totalDemand.toInt)

      buf = keep

      use foreach(message => onNext(message))

      // call ourselves recursively if we didn't send all we needed to
      if (toSend == Int.MaxValue) { deliverBuf() }
    }
  }

}
