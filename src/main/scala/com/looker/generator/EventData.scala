package com.looker.generator

import com.looker.generator.EventManager.EventType
import com.looker.random.WeightedDistribution

import scala.collection.mutable

object EventData {

  val allPrevTypes = Array(
    EventType.Home,     EventType.Category, EventType.Brand,  EventType.Product, EventType.Cart,
    EventType.Purchase, EventType.History,  EventType.Cancel
  )

  val allTypes = allPrevTypes ++ Array( EventType.Login, EventType.Register, EventType.End)

  // Home, Category, Brand, Product, Cart, Purchase, History, Cancel, Login, Register, End
  // 1     2         3      4        5     6         7        8       9      10        11
  // Empty and Full Arrays

  val full           = Array( 1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1)

  // From Home Event Type
  val loggedInBase     = Array( 1,  1,  1,  1,  1,  1,  1,  1,  0,  0,  1) // no registration / login
  val loggedOutBase    = Array( 1,  1,  1,  1,  1,  0,  0,  0,  1,  0,  1) // only login no registration
  val noUserBase       = Array( 1,  1,  1,  1,  1,  0,  0,  0,  0,  1,  1) // no login, but yes registration

  val itemInCartBase   = full                                              // should be able to do anything
  val noItemInCartBase = Array( 1,  1,  1,  1,  1,  0,  1,  1,  1,  1,  1) // because no item in cart, cannot check out

  /* user logged in with item in cart State6 */
  val homeBase         = Array(0,  24, 31,  0,  0,  0,  0,  0,  1,  1, 45)
  val categoryBase     = Array(25,  0,  0, 55,  0,  0,  0,  0,  2,  2, 20)
  val brandBase        = Array(15,  0,  0, 62,  0,  0,  0,  0,  1,  4, 23)
  val productBase      = Array(22, 12, 11, 11, 45,  0,  0,  0,  3,  5, 45)
  val cartBase         = Array(20,  0,  0,  0,  0, 99,  0,  3, 40, 40, 15)
  val historyBase      = Array( 5,  0,  0,  0,  0,  0,  0,  0,  0,  0, 70)
  val cancelBase       = Array(15,  0,  0,  0,  0,  0,  0,  0,  0,  0, 85)
  val purchaseBase     = Array( 5,  0,  0,  0,  0,  0,  4,  0,  0,  0, 91)

  //"Organic", "Email", "Search", "Facebook", "Display"),
  val emailBase        = Array(1.00, 1.00, 1.00, 1.00, 1.00, 1.34, 1.00, 1.00, 1.00, 1.00, 0.85)
  val facebookBase     = Array(1.00, 1.00, 1.00, 1.00, 1.00, 0.95, 1.00, 1.00, 1.00, 1.00, 1.02)
  val displayBase      = Array(1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.35)
  val searchBase       = Array(1.00, 1.00, 1.00, 1.00, 1.00, 1.05, 1.00, 1.00, 1.00, 1.00, 0.70)
  val organicBase      = Array(1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00)

  def trafficLookup(trafficSource : String) : Array[Double] = trafficSource match {
    case "Organic"  => organicBase
    case "Email"    => emailBase
    case "Search"   => searchBase
    case "Facebook" => facebookBase
    case "Display"  => displayBase
  }

  def baseLookup(prev : EventType.Value) : Array[Int] = prev match {
    case EventType.Home     => homeBase
    case EventType.Category => categoryBase
    case EventType.Brand    => brandBase
    case EventType.Product  => productBase
    case EventType.Cart     => cartBase
    case EventType.History  => historyBase
    case EventType.Cancel   => cancelBase
    case EventType.Purchase => purchaseBase
  }

  def loggedInLookup(state: Int) : Array[Int] = state match {
    case 0 => loggedInBase
    case 1 => loggedOutBase
    case 2 => noUserBase
  }

  def cartLookup(state: Int) : Array[Int] = state match {
    case 0 => itemInCartBase
    case 1 => noItemInCartBase
  }

  val firstBase        = Array(1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00)
  val secondBase       = Array(1.00, 1.00, 1.00, 1.00, 1.00, 1.70, 1.00, 1.00, 1.30, 1.30, 1.10)
  val moreBase         = Array(1.00, 1.00, 1.00, 1.00, 1.00, 1.85, 1.00, 1.00, 1.50, 1.40, 1.15)

  def sessionSeqNumber(state: Int) : Array[Double] = state match {
    case 0 => firstBase
    case 1 => secondBase
    case 2 => moreBase
  }

  val noOrders   = Array(1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00)
  val oneOrder   = Array(0.90, 3.30, 3.30, 3.40, 3.50, 3.50, 1.00, 1.00, 1.70, 1.70, 1.00)
  val twoOrder   = Array(0.90, 2.30, 2.30, 2.40, 2.50, 2.50, 1.00, 1.00, 1.60, 1.60, 1.00)
  val threeOrder = Array(0.90, 1.30, 1.30, 1.40, 1.50, 2.50, 1.00, 1.00, 1.50, 1.30, 1.00)

  def sessionMadeOrder(state: Int) : Array[Double] = state match {
    case 0 => noOrders
    case 1 => oneOrder
    case 2 => twoOrder
    case 3 => threeOrder
  }

  def nextActionType(sessionState : SessionState) = {
    val prevState: EventType.Value = sessionState.last.get.eventType
    val userState = if (sessionState.userId.nonEmpty) {
      if (sessionState.userData.loggedIn) { 0 } else { 1 }
    } else { 2 }

    val itemsInCart = if (sessionState.itemsInCart > 0) { 0 } else { 1 }

    val seqNumber = Math.min(sessionState.sequenceNumber, 2)

    val hasMadeOrder = Math.min(sessionState.sequenceNumber, 3)

    val sampler = faster.get((userState, itemsInCart, seqNumber, hasMadeOrder, prevState, sessionState.trafficSource))

    sampler.get.sample
  }

  // map from logged in state (0, 1, 2), and item in cart state (0, 1), previous type, and traffiSource to the weighted
  // randomizer that should be used.
  val faster : mutable.HashMap[(Int, Int, Int, Int, EventType.Value, String), WeightedDistribution[EventType.Value, Double]] = {
    val distributions = allPrevTypes.flatMap(fromEventType => {
      val base = baseLookup(fromEventType).map(_.toDouble)

      (0 to 2).flatMap( loggedInState => {
        val userStatus = loggedInLookup(loggedInState).map(_.toDouble)

        (0 to 1).flatMap( cartState => {
          val cartStatus = cartLookup(cartState).map(_.toDouble)

          (0 to 2).flatMap(seqNumberState => {
            val seqStatus = sessionSeqNumber(seqNumberState)

            (0 to 3).flatMap(hasMadeOrder => {
              val orderDist = sessionMadeOrder(hasMadeOrder)

              Array("Organic", "Email", "Search", "Facebook", "Display").map(trafficSource => {
                val trafficStatus = trafficLookup(trafficSource)
                val status = (loggedInState, cartState, seqNumberState, hasMadeOrder, fromEventType, trafficSource)
                val combined = List(base, userStatus, cartStatus, trafficStatus, seqStatus, orderDist).transpose.map(_.product).toArray

                (status, WeightedDistribution[EventType.Value, Double](allTypes, combined))
              })
            })
          })
        })
      })
    })
    mutable.HashMap(distributions.toSeq : _*)
  }


}
