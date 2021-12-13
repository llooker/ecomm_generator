package com.looker.generator


import akka.actor.{Actor, Props, ActorRef, ActorSystem}
import akka.util.ByteString

import scala.concurrent.ExecutionContext.Implicits.global
import akka.stream._
import akka.stream.scaladsl._

import java.io.{FileWriter, File}

import scala.concurrent.{Promise, Future, Await}
import scala.concurrent.duration._

object Generator {

  def generateProductData(dataDirectory : String) = {
    val products = ProductManager.newReadProducts.map(product => new DataOutput.Product(product))
    val future = Future.sequence(products.map(_.futureString))

    val productData = Await.result(future, 30.seconds)

    val filename = s"$dataDirectory/${DataOutput.Product.tableName.toLowerCase}.csv"
    val fileWriter = new FileWriter(filename)
    productData.foreach(data => fileWriter.write(data + "\n"))
    fileWriter.close
  }

  def generateDistributionCenterData(dataDirectory : String) = {
    val centers = ProductManager.distributionCenters.map(c => new DataOutput.DistributionCenter(c.id, c.name, c.lat, c.long))

    val future = Future.sequence(centers.map(_.futureString).toSeq)

    val centerData = Await.result(future, 30.seconds)

    val filename = s"$dataDirectory/${DataOutput.DistributionCenter.tableName.toLowerCase}.csv"
    val fileWriter = new FileWriter(filename)
    centerData.foreach(data => fileWriter.write(data + "\n"))
    fileWriter.close
  }

  def generate(dataDirectory : String, years : Int, firstMonthVisitBase : Int) = {

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    // determines if all sessions that have been sent to the node have finished.
    val allSessionsTeriminated = Promise[Unit]
    val sessionRequestSource : Source[SessionRequest, ActorRef] = Source.actorPublisher[SessionRequest]( SessionCycle.CycleHead.props(allSessionsTeriminated))
    val sinkActor = system.actorOf(SessionCycle.CycleTail.props, name = "cycleTail")

    val sink = Sink.actorRef(sinkActor, SessionCycle.Done)

    val eventWriter         = FileIO.toFile(new File(s"$dataDirectory/events.csv"))
    val userWriter          = FileIO.toFile(new File(s"$dataDirectory/users.csv"))
    val orderItemWriter     = FileIO.toFile(new File(s"$dataDirectory/order_items.csv"))
    val inventoryItemWriter = FileIO.toFile(new File(s"$dataDirectory/inventory_items.csv"))

    val userCounter          = system.actorOf(Props(new FutureCounter("User")), "userCounter")
    val orderCounter         = system.actorOf(Props(new FutureCounter("Order")), "orderCounter")
    val orderItemCounter     = system.actorOf(Props(new FutureCounter("OrderItem")), "orderItemCounter")
    val inventoryItemCounter = system.actorOf(Props(new FutureCounter("InventoryItem")), "inventoryItemCounter")
    val eventCounter         = system.actorOf(Props(new FutureCounter("Event")), "eventCounter")

    /* Initialize the source */
    val generator = NewVisitorGenerator(years, firstMonthVisitBase)
    val shipmentGenerator = generator.generateShipments()
    val sessionManager = new SessionManager(orderCounter, userCounter, generator.lastDayToGenerate)
    val dataExtractor = new DataExtractor(orderCounter, inventoryItemCounter, eventCounter, sessionManager, generator.lastDayToGenerate)
    /*
     * Very high level:
     * this builds a graph that has two sources:
     *  + shipment requests
     *  + session requests
     *
     *  session requests get built into sessions
     *  a session is then consumed by serveral sinks
     *  + extractors (which extract information about what happened and write them to their respective out file)
     *  + nextSessionFlow (where we will built a NEW session)
     */
    val graph: RunnableGraph[(ActorRef, List[Future[IOResult]])] = RunnableGraph.fromGraph(
      GraphDSL.create(sessionRequestSource, sink, eventWriter, userWriter, orderItemWriter, inventoryItemWriter)(
        (sessionRequestSource, _, o1, o2, o3, o4) => (sessionRequestSource, List(o1, o2, o3, o4))
      ) {
        implicit builder => (source, sink, eventWriter, userWriter, orderItemWriter, inventoryItemWriter) => {
          import GraphDSL.Implicits._
          val shipmentSource = builder.add(shipmentGenerator)
          val extractor = builder.add(dataExtractor.extractorFlow)
          val bcast = builder.add(Broadcast[Session](2))
          val builderFlow = builder.add(sessionManager.builderFlow)
          val nextSessionFlow = builder.add(sessionManager.nextSessionBuilder)
          val shipmentFlow = builder.add(dataExtractor.shipmentFlow)

          val inventoryItemMerger = builder.add(Merge[ByteString](2))

          source ~> builderFlow ~> bcast ~>extractor.in
          shipmentSource ~> shipmentFlow ~> inventoryItemMerger.in(0)

          /* write out to files */
          extractor.out0 ~> eventWriter
          extractor.out1 ~> userWriter
          extractor.out2 ~> inventoryItemMerger.in(1)
          extractor.out3 ~> orderItemWriter

          inventoryItemMerger.out ~> inventoryItemWriter

          /* pass next session to top of loop */
          bcast ~> nextSessionFlow ~> sink

          ClosedShape
        }
      }
    )

    /* this down here is largely plumbing to make the whole thing work.
     * the majority of this is a hack to work around the fact that cycles are apparently
     * very difficult in akka streams. Proceed with caution here.
     *
     * Because cycles are difficult, this cycle is broken into two pieces
     */
    val (ref : ActorRef, sinkFut ) = graph.run

    /* initialize the sink to send messages to the head */
    sinkActor ! SessionCycle.Init(ref)

    val sessionGenerator = generator.generateSessions()

    val out = Sink.actorRef(ref, SessionCycle.Done)

    /* build a flow from source to the source actor (head of loop) */
    sessionGenerator.to(out).run()

    /* wait for it all to finish */
    Await.result(Future.sequence(allSessionsTeriminated.future :: sinkFut), Duration.Inf)

    system.terminate()
  }
}
