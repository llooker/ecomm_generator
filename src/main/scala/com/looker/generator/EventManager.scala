package com.looker.generator

import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import com.looker.random.WeightedDistribution

import scala.concurrent.Future
import scala.util.Random

import com.looker.generator.FutureCounter.IDRequest

object EventManager {

  implicit val timeout = Timeout(5.seconds)

  object EventType extends Enumeration {
    val Home, Category, Product, Brand, Cart, Purchase, History, Cancel, FutureCancel, Return, Login, Register, End = Value
  }

  val facebookInitialLandingPageDist = WeightedDistribution(Array(EventType.Brand, EventType.Product), Array(35, 65))
  val displayInitialLandingPageDist  = WeightedDistribution(Array(EventType.Category, EventType.Brand), Array(53, 47))
  val emailInitialLandingPageDist    = WeightedDistribution(Array(EventType.Category, EventType.Brand, EventType.Product), Array(20, 12, 68))
  val searchInitialLandingPageDist   = WeightedDistribution(Array(EventType.Home, EventType.Category, EventType.Brand, EventType.Product), Array(61, 7, 8, 24))
  val organicInitialLandingPageDist  = WeightedDistribution(Array(EventType.Home, EventType.Category, EventType.Product), Array(75, 11, 13))

  def getInitialLandingPageType(trafficSource: String) : EventType.Value = {
    trafficSource match {
      case "Facebook" => facebookInitialLandingPageDist.sample
      case "Display"  => displayInitialLandingPageDist.sample
      case "Email"    => emailInitialLandingPageDist.sample
      case "Search"   => searchInitialLandingPageDist.sample
      case "Organic"  => organicInitialLandingPageDist.sample
    }
  }

  // TODO flesh this out more
  def getReturnLandingPageType(sessionState : SessionState) : EventType.Value = {
    if (sessionState.userData.loggedIn && Random.nextDouble < 0.4) {
      EventType.History
    } else {
      EventType.Home
    }
  }

  // TODO flesh this out more
  def getCancelLandingPageType(sessionState: SessionState) : EventType.Value = {
    if (sessionState.userData.loggedIn && Random.nextDouble < 0.45) {
      EventType.History
    } else {
      EventType.Home
    }
  }

  val types = Array(EventType.Home, EventType.Category, EventType.Product, EventType.Brand, EventType.Cart, EventType.End)
  val homeFirstActionTypeDistribution     = WeightedDistribution(types, Array(0, 29, 19, 0, 0, 52))
  val categoryFirstActionTypeDistribution = WeightedDistribution(types, Array(40, 0, 0, 20, 0, 40))
  val brandFirstActionTypeDistribution    = WeightedDistribution(types, Array(35, 0, 0, 18, 0, 47))
  val productFirstActionTypeDistribution  = WeightedDistribution(types, Array(5, 7, 7, 18, 42, 21))

  def getFirstActionType(previousType: EventType.Value) : EventType.Value = {
    previousType match {
      case EventType.Home     => homeFirstActionTypeDistribution.sample
      case EventType.Category => categoryFirstActionTypeDistribution.sample
      case EventType.Brand    => brandFirstActionTypeDistribution.sample
      case EventType.Product  => productFirstActionTypeDistribution.sample
    }
  }

  def getCancelEventTypes(sessionState: SessionState) : EventType.Value = {
    val previousType = sessionState.last.get.eventType
    if (sessionState.userData.loggedIn) {
      previousType match {
        case EventType.Home    => EventType.History
        case EventType.History => EventType.FutureCancel
        // Note: this is a cancel event type, because the future cancel is used to create the event
        // but the future cancel event has a cancel event type (only difference is how they update state)
        case EventType.Cancel  => EventType.End
      }
    } else {
      previousType match {
        case EventType.Home => EventType.Login
      }
    }
  }

  def getReturnEventTypes(sessionState: SessionState) : EventType.Value = {
    val previousType = sessionState.last.get.eventType
     if (sessionState.userData.loggedIn) {
       previousType match {
         case EventType.Home   => EventType.History
         case EventType.History => EventType.Return
         case EventType.Return  => EventType.End
       }
     } else {
       previousType match {
         case EventType.Home => EventType.Login
       }
     }
  }

  def addEvent(eventType: EventType.Value, sessionState: SessionState, reversed : List[Event], orderCounter: ActorRef, userCounter : ActorRef): List[Event] = {
    var reversedEvents = reversed
    if (eventType != EventType.End) {
      populateEvents(eventType, sessionState, orderCounter, userCounter).foreach(e => {
        reversedEvents = e :: reversedEvents
        sessionState.updateFromEvent(e)
      })
    }
    reversedEvents
  }

  def generateEvents(sessionState : SessionState, orderCounter : ActorRef, userCounter : ActorRef, revSeedEvents: List[Event], newType : Option[SessionState => EventType.Value] = None): List[Event] = {
    var reversedEvents: List[Event] = revSeedEvents
    var lastLength = reversedEvents.length
    val eventGeneratorFxn = newType.getOrElse(EventData.nextActionType _) // note: partially applied fxn

    do {
      lastLength = reversedEvents.length

      reversedEvents = addEvent(eventGeneratorFxn(sessionState), sessionState, reversedEvents, orderCounter, userCounter)
    } while (lastLength != reversedEvents.length)

    reversedEvents
  }

  def populateEvents(eventType: EventType.Value, sessionState: SessionState, orderCounter: ActorRef, userCounter: ActorRef): List[Event] = {
    val loggedInUserId = if (sessionState.userData.loggedIn) { sessionState.userData.optionalId } else { None }

    eventType match {
      case EventType.Register => {
        val id: Future[Int] = (userCounter ? IDRequest).mapTo[Int]
        val lastEvent = sessionState.last.get

        List(
          RegisterEvent(sessionState.nextTime, userId = Some(id)),
          lastEvent.withUserId(id).withTime(sessionState.nextTime)
        )
      }
      case EventType.Login    => List(
        LoginEvent(sessionState.nextTime, userId = sessionState.userId),
        sessionState.last.get.withUserId(sessionState.userId.get).withTime(sessionState.nextTime)
      )
      case EventType.Category => List(CategoryEvent(pickCategory(Some(sessionState.userData), sessionState.category), sessionState.nextTime, loggedInUserId))
      case EventType.Product  => List(ProductEvent(pickProduct(Some(sessionState.userData), sessionState.category), sessionState.nextTime, loggedInUserId))
      case EventType.Brand    => List(BrandEvent(pickProduct(Some(sessionState.userData), sessionState.category).brand, sessionState.nextTime,  loggedInUserId))
      case EventType.Cart => sessionState.last match {
        case Some(ProductEvent(last_product, _, _, _)) => List(CartEvent(last_product :: sessionState.cart, sessionState.nextTime, loggedInUserId))
        case _ => List(CartEvent(sessionState.cart, sessionState.nextTime, loggedInUserId))
      }
      case EventType.History  => List(HistoryEvent(sessionState.nextTime, loggedInUserId))
      case EventType.Home     => List(HomeEvent(sessionState.nextTime))
      case EventType.Purchase => {
        val id: Future[Int] = (orderCounter ? IDRequest).mapTo[Int]

        List(CompletedPurchaseEvent(sessionState.cart, id, sessionState.nextTime, loggedInUserId))
      }
      case EventType.Cancel   => {
        val orderId: Future[Int] = (orderCounter ? IDRequest).mapTo[Int]
        List(
          CancelledPurchaseEvent(sessionState.cart, orderId, sessionState.nextTime, loggedInUserId),
          HistoryEvent(sessionState.nextTime, loggedInUserId),
          CancelFutureEvent(orderId, sessionState.nextTime, loggedInUserId)
        )
      }
      case EventType.Return   => sessionState.returnedItems.map(p => ReturnEvent(p, sessionState.nextTime, loggedInUserId))
      case EventType.FutureCancel => sessionState.cancelledOrders.map( orderId => CancelFutureEvent(orderId, sessionState.nextTime, loggedInUserId))
    }
  }

  def pickProduct(userData: Option[UserData], categoryOption: Option[Category]) = {
    if (categoryOption.isEmpty) {
      ProductManager.generateProduct(userData)
    } else {
      ProductManager.generateProductInCategory(categoryOption.get)
    }
  }

  def pickCategory(userData: Option[UserData], categoryOption: Option[Category]) = {
    if (categoryOption.isEmpty) {
      ProductManager.generateCategory(userData)
    } else {
      ProductManager.generateBiasedCategory(userData, categoryOption.get)
    }
  }
}
