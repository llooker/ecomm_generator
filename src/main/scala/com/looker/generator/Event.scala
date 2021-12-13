package com.looker.generator

import com.looker.generator.EventManager.EventType
import org.joda.time.DateTime

import scala.concurrent.Future
import scala.util.Random

/* scala. be better. or me. be better. This was way too shitty to be the only way to do this. */

abstract class Event(val eventType: EventType.Value) {
  def withId(newId : Future[Int]) : Event
  def withUserId(newUserId: Future[Int]) : Event
  def withTime(time : DateTime) : Event

  def id : Option[Future[Int]]
  def userId : Option[Future[Int]]
  def time : DateTime

  def detail = ""
  def uri = "/" + eventType.toString.toLowerCase

  def updateSessionState(sessionState : SessionState) : Unit = {}
}

case class CategoryEvent(category: Category, time: DateTime, userId : Option[Future[Int]] = None, id : Option[Future[Int]] = None) extends Event(EventType.Category){
  override def detail = category.name

  override def uri = "/department/" + Department.name(category.department) + "/category/" + category.name

  def withId(newId : Future[Int]) : CategoryEvent = copy(id = Some(newId))
  def withUserId(newUserId : Future[Int]) : CategoryEvent = copy(userId = Some(newUserId))
  def withTime(time : DateTime) : CategoryEvent = copy(time = time)

  override def updateSessionState(sessionState : SessionState) = sessionState.category = Some(category)
}

case class BrandEvent(brand: Brand, time: DateTime, userId : Option[Future[Int]] = None, id : Option[Future[Int]] = None) extends Event(EventType.Brand) {
  override def detail = brand.name

  override def uri = "/department/" + Department.name(brand.category.department) + "/category/" + brand.category.name + "/brand/" + brand.name

  def withId(newId : Future[Int]) : BrandEvent = copy(id = Some(newId))
  def withUserId(newUserId : Future[Int]) : BrandEvent = copy(userId = Some(newUserId))
  def withTime(time : DateTime) : BrandEvent = copy(time = time)

  override def updateSessionState(sessionState : SessionState) = sessionState.category = Some(brand.category)
}

case class CartEvent(cart: List[Product], time: DateTime, userId : Option[Future[Int]] = None, id : Option[Future[Int]] = None) extends Event(EventType.Cart) {
  def withId(newId : Future[Int]) : CartEvent = copy(id = Some(newId))
  def withUserId(newUserId : Future[Int]) : CartEvent = copy(userId = Some(newUserId))
  def withTime(time : DateTime) : CartEvent = copy(time = time)

  override def updateSessionState(sessionState: SessionState) = {
    sessionState.cart = cart
    sessionState.touchedCart = true
  }
}
case class HistoryEvent(time: DateTime, userId : Option[Future[Int]] = None, id : Option[Future[Int]] = None) extends Event(EventType.History) {
  def withId(newId : Future[Int]) : HistoryEvent = copy(id = Some(newId))
  def withUserId(newUserId : Future[Int]) : HistoryEvent = copy(userId = Some(newUserId))
  def withTime(time : DateTime) : HistoryEvent = copy(time = time)
}
case class HomeEvent(time: DateTime, userId : Option[Future[Int]] = None, id : Option[Future[Int]] = None) extends Event(EventType.Home) {
  def withId(newId : Future[Int]) : HomeEvent = copy(id = Some(newId))
  def withUserId(newUserId : Future[Int]) : HomeEvent = copy(userId = Some(newUserId))
  def withTime(time : DateTime) : HomeEvent = copy(time = time)
}

trait PurchaseEvent {
  def purchasedItems:List[Product]
  def cancelled: Boolean
  def returnedItems: List[Product]
  def userId: Option[Future[Int]]
  def orderId: Future[Int]
  def time: DateTime

  def updateSessionState(sessionState: SessionState) = {
    sessionState.cart = List()
    sessionState.madePurchase   = true
    sessionState.purchaseEvents = this :: sessionState.purchaseEvents
  }
}

case class CancelFutureEvent(orderId: Future[Int], time: DateTime, userId : Option[Future[Int]] = None, id : Option[Future[Int]] = None) extends Event(EventType.Cancel) {
  def withId(newId : Future[Int]) : CancelFutureEvent = copy(id = Some(newId))
  def withUserId(newUserId : Future[Int]) : CancelFutureEvent = copy(userId = Some(newUserId))
  def withTime(time : DateTime) : CancelFutureEvent = copy(time = time)

  override def updateSessionState(sessionState: SessionState) = {
    sessionState.cancelledOrders = List()
  }
}

case class CancelledPurchaseEvent(purchasedItems: List[Product], orderId: Future[Int], time: DateTime, userId : Option[Future[Int]] = None, id : Option[Future[Int]] = None) extends Event(EventType.Cancel) with PurchaseEvent {
  val cancelled = true
  val returnedItems:List[Product] = List()

  def withId(newId : Future[Int]) : CancelledPurchaseEvent = copy(id = Some(newId))
  def withUserId(newUserId : Future[Int]) : CancelledPurchaseEvent = copy(userId = Some(newUserId))
  def withTime(time : DateTime) : CancelledPurchaseEvent = copy(time = time)

  override def updateSessionState(sessionState: SessionState) = {
    super.updateSessionState(sessionState)

    sessionState.cancelledInSession = true
    sessionState.cancelledPurchase  = true
    sessionState.cancelledEvents    = this :: sessionState.cancelledEvents
  }
}

case class CompletedPurchaseEvent(purchasedItems: List[Product], orderId: Future[Int], time: DateTime, userId : Option[Future[Int]] = None, id : Option[Future[Int]] = None) extends Event(EventType.Purchase) with PurchaseEvent {
  val returnedItems: List[Product] = purchasedItems.collect{ case i if Random.nextDouble <= .01 => i }
  val cancelled = Random.nextDouble <= 0.02

  def withId(newId : Future[Int]) : CompletedPurchaseEvent = copy(id = Some(newId))
  def withUserId(newUserId : Future[Int]) : CompletedPurchaseEvent = copy(userId = Some(newUserId))
  def withTime(time : DateTime) : CompletedPurchaseEvent = copy(time = time)

  override def updateSessionState(sessionState: SessionState) = {
    super.updateSessionState(sessionState)
    if (cancelled) {
      sessionState.futureCancelledEvents = this :: sessionState.futureCancelledEvents
      sessionState.cancelledPurchase = true
      sessionState.cancelledEvents   = this :: sessionState.cancelledEvents
    } else if (returnedItems.nonEmpty) {
      sessionState.returnedItems  = returnedItems ++ sessionState.returnedItems
      sessionState.returnedEvents =  this :: sessionState.returnedEvents
      sessionState.madeReturn     = true
    }
  }
}

case class ProductEvent(product: Product, time: DateTime, userId : Option[Future[Int]] = None, id : Option[Future[Int]] = None) extends Event(EventType.Product) {
  override def detail = product.id.toString
  override def uri = "/product/" + product.id.toString

  def withId(newId : Future[Int]) : ProductEvent = copy(id = Some(newId))
  def withUserId(newUserId : Future[Int]) : ProductEvent = copy(userId = Some(newUserId))
  def withTime(time : DateTime) : ProductEvent = copy(time = time)

  override def updateSessionState(sessionState: SessionState) = {
    sessionState.category = Some(product.category)
    sessionState.product = Some(product)
    sessionState.touchedProduct = true
  }
}

case class ReturnEvent(product: Product, time: DateTime, userId : Option[Future[Int]] = None, id : Option[Future[Int]] = None) extends Event(EventType.Return) {
  override def detail = product.id.toString
  override def uri = "/return"

  def withId(newId : Future[Int]) : ReturnEvent = copy(id = Some(newId))
  def withUserId(newUserId : Future[Int]) : ReturnEvent = copy(userId = Some(newUserId))
  def withTime(time : DateTime) : ReturnEvent = copy(time = time)

  override def updateSessionState(sessionState: SessionState) = {
    sessionState.returnedItems = List()
  }
}

case class RegisterEvent(time: DateTime, userId : Option[Future[Int]] = None, id : Option[Future[Int]] = None) extends Event(EventType.Register) {
  def withId(newId : Future[Int]) : RegisterEvent = copy(id = Some(newId))
  def withUserId(newUserId : Future[Int]) : RegisterEvent = copy(userId = Some(newUserId))
  def withTime(time : DateTime) : RegisterEvent = copy(time = time)

  override def updateSessionState(sessionState: SessionState) = {
    sessionState.userData = sessionState.userData.withId(userId.get).login
    sessionState.userCreatedTime = Some(time)
  }
}
case class LoginEvent(time: DateTime, userId : Option[Future[Int]], id: Option[Future[Int]] = None) extends Event(EventType.Login) {
  def withId(newId : Future[Int]) : LoginEvent = copy(id = Some(newId))
  def withUserId(newUserId : Future[Int]) : LoginEvent = copy(userId = Some(newUserId))
  def withTime(time : DateTime) : LoginEvent = copy(time = time)

  override def updateSessionState(sessionState: SessionState) = {
    sessionState.userData = sessionState.userData.login
  }
}
