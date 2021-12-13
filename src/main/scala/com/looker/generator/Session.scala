package com.looker.generator

import akka.NotUsed
import akka.actor.{ActorRef, Props, ActorSystem}
import akka.stream._
import akka.stream.scaladsl._
import breeze.stats.distributions.Gamma
import com.looker.generator.EventManager.EventType
import com.looker.generator.SessionCycle.SessionMessage
import com.looker.random.WeightedDistribution
import com.looker.utils.{GraphUtils, DateUtils}
import org.joda.time.{DateTimeConstants, DateTimeZone, DateTime, LocalDate}

import scala.concurrent.Future
import scala.util.Random

trait DataRequest

object SessionHistory {
  def first = SessionHistory(0, 0, None)
}

case class SessionHistory(
  totalSessions : Int,
  totalPurchases : Int,
  userData : Option[UserData],
  loggedIn : Boolean = false,
  ordersToCancel : List[Future[Int]] = List(),
  productsToReturn : List[Product] = List()
)

object SessionRequest {
  def first(day : LocalDate) = SessionRequest(day, "Standard", SessionHistory.first)
}

case class SessionRequest(day : LocalDate, sessionType : String, sessionHistory : SessionHistory) extends SessionMessage {

  def firstSession = sessionHistory.totalSessions == 0

  def generateSessionState : SessionState = {
    val userData = sessionHistory.userData.get

    SessionState(
      sessionHistory.totalSessions + 1,
      sessionHistory.totalPurchases,
      userData,
      sessionHistory.loggedIn,
      DateUtils.generateSessionStartTime(day, userData),
      userData.trafficSource
    )
  }
}

case class ShipmentRequest(day : LocalDate, items : Int) extends DataRequest

trait WithUserData {
  def day : DateTime
  def userData : UserData
  val nextSequenceNumber : Int
}

case class Session(
  sequenceNumber: Int = 0,
  day: DateTime,
  userData: UserData,
  events: List[Event],
  sessionState : SessionState
) extends WithUserData {
  val previousSequenceNumber = sequenceNumber - 1
  val nextSequenceNumber     = sequenceNumber + 1
  val trafficSource = sessionState.trafficSource
}

case class SessionState(
  var sequenceNumber     : Int,
  var orderCount         : Int,
  var userData           : UserData,
  var loggedIn           : Boolean,
  var currentTime        : DateTime,
  var trafficSource      : String,
  var cart               : List[Product]            = List(),
  var category           : Option[Category]         = None,
  var department         : Option[Department.Value] = None,
  var product            : Option[Product]          = None,
  var last               : Option[Event]            = None,
  var userCreatedTime    : Option[DateTime]         = None,
  var madePurchase       : Boolean                  = false,
  var madeReturn         : Boolean                  = true,
  var cancelledPurchase  : Boolean                  = false,
  var cancelledInSession : Boolean                  = false,
  var touchedCart        : Boolean                  = false,
  var touchedProduct     : Boolean                  = false,
  var returnedItems      : List[Product]            = List(),
  var purchaseEvents     : List[PurchaseEvent]      = List(),
  var cancelledEvents    : List[PurchaseEvent]      = List(),
  var futureCancelledEvents : List[PurchaseEvent]   = List(),
  var returnedEvents     : List[PurchaseEvent]      = List(),
  var cancelledOrders    : List[Future[Int]]         = List()
) {
  def itemsInCart = cart.length

  def userId = userData.optionalId

  def logout() = loggedIn = false
  def login() = loggedIn = true

  def nextTime = {
    val seconds = Random.nextInt(60) + 15

    currentTime = currentTime.plusSeconds(seconds)

    currentTime
  }

  def incrementOrderCount() = orderCount = orderCount + 1

  def toSessionHistory : SessionHistory = {
    SessionHistory(sequenceNumber, orderCount, Some(userData), loggedIn, cancelledOrders, returnedItems)
  }

  def updateFromEvent(event : Event) = {
    last = Some(event)
    event.updateSessionState(this)
  }
}

class SessionManager(orderCounter : ActorRef, userCounter : ActorRef, lastDate : LocalDate) {

//  /* some inner classes used in the production of a session */
  private case class SessionLocation(day: LocalDate, location: UserLocation) {

    def withUserData(userData: UserData) = {
      val time = DateUtils.generateSessionStartTime(day, userData)
      SessionUserData(time, location, userData)
    }
  }

  case class SessionUserData(day: DateTime, location: UserLocation, userData: UserData) extends WithUserData {
    val nextSequenceNumber = 0
  }

  val builderFlow : Flow[SessionRequest, Session, NotUsed] = Flow[SessionRequest].map { sessionRequest =>
    val newSessionRequest = if (sessionRequest.sessionHistory.userData.isEmpty) {
      val location = LocationManager.pickLocation()
      val userData = UserManager.generateUserData(sessionRequest.day, location)
      val newHistory = sessionRequest.sessionHistory.copy(userData = Some(userData))

      sessionRequest.copy(sessionHistory = newHistory)
    } else {
      sessionRequest
    }

    val sessionState = newSessionRequest.generateSessionState

    newSessionRequest.sessionType match {
      case "Standard" if sessionRequest.firstSession => generateFirstSession(sessionState)
      case "Standard" => generateSubsequentSession(sessionState)
      case "Return" => generateReturnSession(sessionState)
      case "Cancel" => generateCancelSession(sessionState)
    }
  }


  def generateCancelSession(sessionState: SessionState) : Session = {
    var reversed = EventManager.addEvent(EventManager.getCancelLandingPageType(sessionState), sessionState, List(), orderCounter, userCounter)

    reversed = EventManager.generateEvents(sessionState, orderCounter, userCounter, reversed, Some(EventManager.getCancelEventTypes))

    // chance of user continuing on to make order after return
    if (Random.nextDouble() < 0.09) {
      reversed = EventManager.addEvent(EventType.Home, sessionState, reversed, orderCounter, userCounter)
      reversed = EventManager.generateEvents(sessionState, orderCounter, userCounter, reversed)
    }

    Session(sessionState.sequenceNumber, sessionState.currentTime, sessionState.userData, reversed.reverse, sessionState)
  }

  // slightly more contrived example as we forcing the hand of the user rather than letting a markov model govern the actions
  def generateReturnSession(sessionState: SessionState) : Session = {
    var reversed = EventManager.addEvent(EventManager.getReturnLandingPageType(sessionState), sessionState, List(), orderCounter, userCounter)

    reversed = EventManager.generateEvents(sessionState, orderCounter, userCounter, reversed, Some(EventManager.getReturnEventTypes))

    // chance of user continuing on to make order after return
    if (Random.nextDouble() < 0.07) {
      reversed = EventManager.addEvent(EventType.Home, sessionState, reversed, orderCounter, userCounter)
      reversed = EventManager.generateEvents(sessionState, orderCounter, userCounter, reversed)
    }

    Session(sessionState.sequenceNumber, sessionState.currentTime, sessionState.userData, reversed.reverse, sessionState)
  }

  def generateFirstSession(sessionState: SessionState) : Session = {
    //var sessionState = SessionState(sessionRequest.sessionHistory.userData, sessionRequest.day, session.userData.trafficSource)

    var reversed : List[Event] = EventManager.addEvent(EventManager.getInitialLandingPageType(sessionState.trafficSource), sessionState, List(), orderCounter, userCounter)

    reversed = EventManager.addEvent(EventManager.getFirstActionType(sessionState.last.get.eventType), sessionState, reversed, orderCounter, userCounter)

    reversed = EventManager.generateEvents(sessionState, orderCounter, userCounter, reversed)

    Session(sessionState.sequenceNumber, sessionState.currentTime, sessionState.userData, reversed.reverse, sessionState)
  }

  def generateSubsequentSession(sessionState : SessionState) : Session = {
    var reversed : List[Event] = EventManager.addEvent(EventManager.getInitialLandingPageType(sessionState.trafficSource), sessionState, List(), orderCounter, userCounter)

    reversed = EventManager.addEvent(EventManager.getFirstActionType(sessionState.last.get.eventType), sessionState, List(), orderCounter, userCounter)

    reversed = EventManager.generateEvents(sessionState, orderCounter, userCounter, reversed)

    Session(sessionState.sequenceNumber, sessionState.currentTime, sessionState.userData, reversed.reverse, sessionState)
  }

  /*
   * In this case we don't NEED to create another session, but we may want
   * to take note that the order was cancelled and use that
   * to decide if we make more purchases. we have already made note of the
   * events where the user went to their history page and then the cancel page.
   */
  val nextSessionCancelledInSession = Flow[Session].map( session => {
    if (Random.nextDouble() < 0.4) {
      SessionCycle.SessionTerminated(session.sequenceNumber)
    } else {
      val nextDate = DateUtils.generateNextSessionDateLambda(session.day.toLocalDate(), 2, 45)

      val sessionState = session.sessionState

      if (Random.nextDouble() < 0.6) { sessionState.logout() }

      SessionRequest(nextDate, "Standard", sessionState.toSessionHistory)
    }
  })

  /*
   * In this case we NEED to create another session where the user
   * cancels their order.
   *
   * We have already written out the order and the order was marked as cancelled.
   * Therefore we need to add the corresponding events that indicate that the
   * user somehow came back to the site and cancelled their order.
   *
   */
  val nextSessionCancelledInFuture = Flow[Session].map( session => {
    //val cancelledOrders = session.sessionState.futureCancelledEvents.map(e => e.orderId)

    val newDate = DateUtils.generateNextSessionDateLinear(session.day.toLocalDate(), 5)

    if (Random.nextDouble() < 0.3) {  session.sessionState.logout() }

    SessionRequest(newDate, "Cancel", session.sessionState.toSessionHistory)
  })

  /*
   * At this point, we have already marked certain items to be returned.
   * Create another session to return them.
   */
  val nextSessionReturnedItems = Flow[Session].map( session => {
    //val products = session.sessionState.returnedItems
    val newDate = DateUtils.generateNextSessionDateLinear(session.day.toLocalDate(), 15, 10)

    if (Random.nextDouble() < 0.8) {  session.sessionState.logout() }

    SessionRequest(newDate, "Cancel", session.sessionState.toSessionHistory)
  })

  /*
   * Here the user made an order and did not return anything from it
   * nor did they cancel the order.
   */
  val nextSessionMadeOrder = Flow[Session].map( session => {
    val random = Random.nextDouble()
    if ((random < 0.35 && session.userData.trafficSource == "Organic") || random < 0.25 ) {
      SessionCycle.SessionTerminated(session.sequenceNumber)
    } else {
      // when the user comes back
      val nextDate = DateUtils.generateNextSessionDateLambda(session.day.toLocalDate, 2, 10)

      val sessionState = session.sessionState

      sessionState.incrementOrderCount()

      if (Random.nextDouble() < 0.2) { sessionState.logout() }

      SessionRequest(nextDate, "Standard", sessionState.toSessionHistory)
    }
  })

  /*
   * this is a user who did not make an order.
   */
  val nextSessionDidNotMakeOrder = Flow[Session].map( session => {
    val churnProbability = if (session.sessionState.userCreatedTime.nonEmpty) {
      0.3
    } else if (session.sessionState.userData.optionalId.nonEmpty) {
      0.1
    } else if (session.events.length == 1 && session.events.head.eventType == EventType.Home) {
      0.90 // if they hit the home page and immediately left just drop the user.
    } else if (session.sessionState.touchedCart) {
      0.6 // if they touched the cart
    } else if (session.sessionState.touchedProduct) {
      0.8
    } else {
      0.9
    }

    if (Random.nextDouble() < churnProbability) {
      SessionCycle.SessionTerminated(session.sequenceNumber)
    } else {
      val nextDate = DateUtils.generateNextSessionDateLambda(session.day.toLocalDate, 4, 6)
      val sessionState = session.sessionState

      if (Random.nextDouble() < 0.6) { sessionState.logout() }

      SessionRequest(nextDate, "Standard", sessionState.toSessionHistory)
    }
  })

  /*
   * From a session, determines if there should be a next session. If so, passes this session on.
   * If not, passes a cancelled session on.
   */
  val nextSessionBuilder = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val madeOrderSplitter             = builder.add(GraphUtils.filterSplitter[Session](_.sessionState.madePurchase))
    val cancelledOrderSplitter        = builder.add(GraphUtils.filterSplitter[Session](_.sessionState.cancelledPurchase))
    val currentCancelledOrderSplitter = builder.add(GraphUtils.filterSplitter[Session](_.sessionState.cancelledInSession))
    val returnedItemSplitter          = builder.add(GraphUtils.filterSplitter[Session](_.sessionState.madeReturn))

    // merges all the sessions back together after splitting them
    val sessionMerger = builder.add(Merge[SessionCycle.SessionMessage](5))

    val filterer: Flow[SessionMessage, SessionMessage, NotUsed] = Flow[SessionMessage].map {
      case term: SessionCycle.SessionTerminated => term
      case sessionRequest: SessionRequest if sessionRequest.day.isAfter(lastDate) => SessionCycle.SessionTerminated(
        sessionRequest.sessionHistory.totalSessions
      )
      case sessionRequest: SessionRequest => sessionRequest
    }

    val sessionFutureFilterer = builder.add(filterer)

    // users who have made orders and cancelled them in the session
    madeOrderSplitter ~> cancelledOrderSplitter ~> currentCancelledOrderSplitter ~> nextSessionCancelledInSession ~> sessionMerger

    // users who made orders and cancelled them in the future -- next session needs to be a cancel
    currentCancelledOrderSplitter.out(1) ~> nextSessionCancelledInFuture ~> sessionMerger

    // users who made an order and returned some of the itmes -- next session needs to be a return
    cancelledOrderSplitter.out(1) ~> returnedItemSplitter ~> nextSessionReturnedItems ~> sessionMerger

    // all the users who made orders and kept the stuff
    returnedItemSplitter.out(1) ~> nextSessionMadeOrder ~> sessionMerger

    // all the users who did not make an order
    madeOrderSplitter.out(1) ~> nextSessionDidNotMakeOrder ~> sessionMerger

    sessionMerger.out ~> sessionFutureFilterer

    FlowShape(madeOrderSplitter.in, sessionFutureFilterer.outlet)
  })
}
