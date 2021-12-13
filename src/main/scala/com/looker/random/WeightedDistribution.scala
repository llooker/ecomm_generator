package com.looker.random

import scala.annotation.tailrec
import scala.util.Random

object WeightedDistribution {
  def apply[T, U : Numeric](keys: Array[T], weights:  Array[U]) = new WeightedDistribution(keys, weights)
}

class WeightedDistribution[T, U : Numeric](keys: Array[T], weights: Array[U]){
  import Numeric.Implicits._

  if (weights.length != keys.length) {
    throw new Exception("Illegal Argument Exception: length of keys does not equal length of values")
  }

  private val length = weights.length
  private val values : Array[Double] = normalize( weights )
  private val csum : Array[Double] = cumulative_sum(values)

  def sample : T = {
    find(Random.nextDouble(), 0, length - 1)
  }

  @tailrec
  private def find(random: Double, lhs: Int, rhs: Int) : T = {
    val ptr = (lhs + rhs) / 2

    if (csum(ptr) < random) {
      find(random, ptr + 1, rhs)
    } else if (csum(ptr) - values(ptr) > random) {
      find(random, lhs, ptr - 1)
    } else {
      keys(ptr)
    }
  }

  private def normalize(values: Array[U]) : Array[Double] = {
    val sum = values.map(e => e.toDouble).sum
    values.map {v => v.toDouble / sum }
  }

  private def cumulative_sum(normalized_values: Array[Double]) : Array[Double] = {
    var total = 0.0
    values.map{e => total += e; total}
  }
}
