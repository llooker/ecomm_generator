package com.looker.utils

import akka.NotUsed
import akka.stream.{FlowShape, FanOutShape2, Graph, UniformFanOutShape}
import akka.stream.scaladsl.{UnzipWith, GraphDSL, Broadcast, Flow}
import GraphDSL.Implicits._

object GraphUtils {
  def filterSplitter[T](function : T => Boolean): Graph[UniformFanOutShape[T, T], NotUsed] = {
    GraphDSL.create() { implicit builder =>

      val broad = builder.add(Broadcast[T](2))

      val filterYes = builder.add(Flow[T].filter(function(_)))
      val filterNo = builder.add(Flow[T].filter(!function(_)))

      broad ~> filterYes
      broad ~> filterNo

      UniformFanOutShape(broad.in, filterYes.outlet, filterNo.outlet)
    }
  }
}
