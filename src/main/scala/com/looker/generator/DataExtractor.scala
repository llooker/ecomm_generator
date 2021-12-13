package com.looker.generator

import akka.NotUsed
import akka.actor.ActorRef
import akka.stream.{FanOutShape4, FlowShape}
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import com.looker.generator.DataOutput.{InventoryItem, User}
import com.looker.utils.{DateUtils, MiscUtils}
import org.joda.time.{DateTimeConstants, Days, LocalDate, DateTime}
import scala.concurrent.duration._

import akka.pattern.ask
import scala.concurrent.ExecutionContext.Implicits.global


import com.looker.generator.FutureCounter.IDRequest

import scala.util.Random

class DataExtractor(orderItemCounter : ActorRef, inventoryItemCounter : ActorRef, eventCounter : ActorRef, sessionManager : SessionManager, lastDate : LocalDate) {

  implicit val timeout = Timeout(5.seconds)

  /* From a given session, determines if there were any new users created */
  /* if so, outputs any new users as bytestrings that can be written out to a file */
  val userExtractor = Flow.fromGraph(GraphDSL.create() {implicit builder =>
    import GraphDSL.Implicits._

    val filterNewUsersFlow: Flow[Session, Session, NotUsed] = Flow[Session].filter(session => { session.sessionState.userCreatedTime.nonEmpty})

    val userConstructor: Flow[Session, User, NotUsed] = Flow[Session].map { s =>
      User(s.sessionState.userCreatedTime.get, s.sessionState.userData)
    }

    val userFormatter : Flow[User, ByteString, NotUsed] = Flow[User].mapAsyncUnordered(4)(_.futureString.map(s => ByteString(s + "\n")))

    val in = builder.add(filterNewUsersFlow)

    val out = builder.add(userFormatter)

    in ~> userConstructor ~> out

    FlowShape(in.in, out.outlet)
  })

  /* From a session, extracts the events that occured and flattens them into a stream of byte strings
   * that can be written out to a file
   */
  val eventExtractor = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val eventFlattener : Flow[Session, DataOutput.Event, NotUsed] = Flow[Session].mapConcat(session => {
      val sessionId = java.util.UUID.randomUUID.toString
      var counter = 0
      session.events.map ( e => {
        val futureId = (eventCounter ? IDRequest).mapTo[Int]

        counter += 1
        DataOutput.Event(session, e.withId(futureId), sessionId, counter)
      })
    })

    val eventFormatter : Flow[DataOutput.Event, ByteString, NotUsed] = Flow[DataOutput.Event].mapAsyncUnordered(4)(_.futureString.map(s => ByteString(s + "\n")))

    val in = builder.add(eventFlattener)
    val out = builder.add(eventFormatter)

    in ~> out

    FlowShape(in.in, out.outlet)
  })

  val orderBuilder = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    /* filter out sessions that don't make purchases */
    val filterOrders: Flow[Session, Session, NotUsed] = Flow[Session].filter(session => session.sessionState.madePurchase)

    def createOrder(event: PurchaseEvent, session: Session) : List[(DataOutput.InventoryItem, DataOutput.OrderItem)] = {
      event.purchasedItems.map(product => {
        val futureInventoryItemId = (inventoryItemCounter ? IDRequest).mapTo[Int]
        val futureOrderItemId = (orderItemCounter ? IDRequest).mapTo[Int]

        val inventoryItemCreated = event.time.minusDays(ProductManager.generateAgeBeforeSale(product)).toLocalDate
        val inventoryItem = DataOutput.InventoryItem(futureInventoryItemId, product, inventoryItemCreated, Some(event.time))

        // TODO figure out how to make this part of when the user comes back
        val returnedAt = if (!event.cancelled && event.returnedItems.contains(product)) {
          Some(event.time)
        } else {
          None
        }

        val eventDate = event.time.toLocalDate
        val daysUntilEnd = Days.daysBetween(lastDate, eventDate).getDays

        val delayed = Random.nextDouble > 0.1

        val processingDays = if (!delayed) {
          2
        } else {
          val base = if (product.distributionCenter.id % 2 == 0) { 2 } else { 3 }

          val rand = Random.nextDouble

          if (rand < 0.63) {
            base
          } else if (rand < 0.85) {
            base + 1
          } else if (rand < 0.95) {
            base + 2
          } else if (rand < 0.98) {
            base + 3
          } else {
            base + 8
          }
        }

        var shipDate: Option[LocalDate] = Some(new LocalDate(eventDate).plusDays(DateUtils.nearestWeekDayOffset(eventDate, processingDays)))

        val miles = MiscUtils.haversineDistance((product.distributionCenter.lat, product.distributionCenter.long), (session.userData.location.lat, session.userData.location.long))
        val base: Int = 3 + (miles / 1000).toInt

        val rand = Random.nextDouble

        // dont delay shipping if processing was delayed
        val delay: Int = if (rand > 0.6 || delayed) {
          0
        } else if (rand > 0.8) {
          1
        } else if (rand > 0.95) {
          2
        } else if (rand > 0.99) {
          3
        } else {
          4
        }

        var deliveryDate : Option[LocalDate] = Some(new LocalDate(shipDate.get).plusDays(DateUtils.nearestWeekDayOffset(shipDate.get, base + delay)))

        val orderItemStatus = if (event.cancelled) {
          shipDate = None
          deliveryDate = None
          "Cancelled"
        } else if (returnedAt.nonEmpty) {
          "Returned"
        } else if (shipDate.get.isAfter(lastDate)) {
          shipDate = None
          deliveryDate = None
          "Processing"
        } else if (deliveryDate.get.isAfter(lastDate)) {
          deliveryDate = None
          "Shipped"
        } else {
          "Complete"
        }

        val orderItem = DataOutput.OrderItem(futureOrderItemId, event.orderId, event.userId.get, product.retailPrice, futureInventoryItemId, orderItemStatus, event.time, returnedAt, shipDate, deliveryDate)

        (inventoryItem, orderItem)
      })
    }

    val orderExtractor: Flow[Session, (DataOutput.InventoryItem, DataOutput.OrderItem), NotUsed] = Flow[Session].mapConcat(session => {
      session.events.collect {
        case event : PurchaseEvent => createOrder(event, session)
      }.flatten
    })

    val in = builder.add(filterOrders)
    val out = builder.add(orderExtractor)

    in ~> out

    FlowShape(in.in, out.outlet)
  })

  val shipmentFlow = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val x = builder.add(Flow[ShipmentRequest].mapConcat(req => {
      (0 to req.items).map { index =>
        val futureInventoryItemId = (inventoryItemCounter ? IDRequest).mapTo[Int]
        DataOutput.InventoryItem(futureInventoryItemId, ProductManager.generateProduct(None), req.day, None )
      }
    }))


    val inventoryItemFormatter = builder.add(
      Flow[DataOutput.InventoryItem].mapAsyncUnordered(4)(_.futureString.map(s => ByteString(s + "\n")))
    )

    x ~> inventoryItemFormatter

    FlowShape(x.in, inventoryItemFormatter.outlet)
  }


  val extractorFlow = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val orderItemFormatter = Flow[DataOutput.OrderItem].mapAsyncUnordered(4)(_.futureString.map(s => ByteString(s + "\n")))

    val inventoryItemFormatter = Flow[DataOutput.InventoryItem].mapAsyncUnordered(4)(_.futureString.map(s => ByteString(s + "\n")))

    val unzip = builder.add(Unzip[DataOutput.InventoryItem, DataOutput.OrderItem])
    val sessionBroadcast = builder.add(Broadcast[Session](3))

    val out1: FlowShape[Session, ByteString] = builder.add(eventExtractor)
    val out2: FlowShape[Session, ByteString] = builder.add(userExtractor)
    val out3: FlowShape[DataOutput.InventoryItem, ByteString]  = builder.add(inventoryItemFormatter)
    val out4: FlowShape[DataOutput.OrderItem, ByteString]      = builder.add(orderItemFormatter)

    /* flatten events from session and write them */
    sessionBroadcast ~> out1.in
    /* write out any new users created */
    sessionBroadcast ~> out2.in
    /* also  build orderitems and inventoryitems */
    sessionBroadcast ~> orderBuilder ~> unzip.in


    unzip.out0 ~> out3.in
    unzip.out1 ~> out4.in

    new FanOutShape4(sessionBroadcast.in, out1.outlet, out2.outlet, out3.outlet, out4.outlet)
  }

}
