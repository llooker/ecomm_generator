package com.looker.generator

import akka.NotUsed
import akka.stream.scaladsl.{Concat, Source}
import breeze.stats.distributions
import com.looker.utils.DateUtils

import org.joda.time.{DateTimeConstants, Days, LocalDate, Months}

object NewVisitorGenerator {
  def apply(years: Int, firstMonthVisits: Int) = new NewVisitorGenerator(years, firstMonthVisits)
}

class NewVisitorGenerator(years: Int, firstMonthVisits: Int) {
  val yearsToGenerate : Int = years + 1
  val lastDayToGenerate : LocalDate = LocalDate.now()
  val firstDayToGenerate = new LocalDate(lastDayToGenerate.getYear - years, 1, 1)

  def generateSessions(): Source[SessionRequest, NotUsed] = {
    Source(0 until yearsToGenerate).map{yearIndex =>
      generateYear[SessionRequest](yearsToGenerate - yearIndex - 1, generateDayVisits)
    }.flatMapConcat(identity)
  }

  def generateShipments(): Source[ShipmentRequest, NotUsed] = {
    Source(0 until yearsToGenerate).map{yearIndex =>
      generateYear[ShipmentRequest](yearsToGenerate - yearIndex - 1, generateDayShipments)
    }.flatMapConcat(identity)
  }

  private val MonthMultiplier = Array(0.97, 0.97, 0.97, 0.97, 0.87, 0.87, 0.87, 0.97, 1.07, 1.07, 1.17, 1.27)
  /*
   * Determines on a given day, the number of NEW visitors that will come to the site.
   *
   * createdInMonth: init *  1.35 ^ (month_num / 12)
   * createdOnDay = createdInMonth / lengthOfMonth(days)
   *
   * TODO: Improve seasonal trend?
   *
   * Add some random noise to the distribution to make it look better.
   */
  private def newFirstVisits(day: LocalDate): Int = {
    val year = day.getYear
    val monthNum = Months.monthsBetween(firstDayToGenerate, day).getMonths

    val baseGeneratedInMonth = firstMonthVisits * Math.pow(1.45, monthNum.toDouble/12)

    // seasonal trend
    val generatedInMonth = baseGeneratedInMonth * MonthMultiplier(day.getMonthOfYear - 1)

    val generatedOnDayBase = generatedInMonth / day.dayOfMonth().getMaximumValue

    val multiplier = day.getDayOfWeek match {
      case DateTimeConstants.MONDAY    => 1.25
      case DateTimeConstants.TUESDAY   => 1.15
      case DateTimeConstants.WEDNESDAY => 1.05
      case DateTimeConstants.THURSDAY  => 1.05
      case DateTimeConstants.FRIDAY    => 0.9
      case DateTimeConstants.SATURDAY  => 0.5
      case DateTimeConstants.SUNDAY    => 0.5
    }

    val bonusMultiplier = if (day.isEqual(DateUtils.blackFriday(day.getYear))) {
      1.15
    } else if (day.isEqual(DateUtils.cyberMonday(day.getYear))) {
      1.16
    } else if (day.getMonthOfYear == 11 && day.getDayOfMonth > 5 && day.getDayOfMonth < 18) {
      1.0 //0.5 + day.getDayOfMonth.toDouble - 5 / 10
    } else {
      1.0
    }

    val generatedOnDay = generatedOnDayBase * multiplier * bonusMultiplier

    // sample a gaussian distribution with mean of x, and standard dev of y
    distributions.Gaussian(generatedOnDay, 10).sample().toInt
  }

  private def generateDayVisits(day: LocalDate): Source[SessionRequest, NotUsed] = {
    val newVisits = newFirstVisits(day)

    Source(0 to newVisits).map(_ => SessionRequest.first(day))
  }

  private def generateDayShipments(day: LocalDate) : Source[ShipmentRequest, NotUsed] = {
    val daysLeft = Days.daysBetween(day, lastDayToGenerate).getDays

    if (daysLeft < 135) {
      val base = (4.41 * firstMonthVisits.toFloat / 10) / (1 + Math.exp(-1 * ((135 - daysLeft - 110).toFloat / 6)))
      val noiseBase = Math.min(15, base / 10)
      val withNoise = distributions.Gaussian(base, noiseBase).sample().toInt

      val shipmentSource = Source.single(ShipmentRequest(day, withNoise))

      shipmentSource
    } else {
      Source.empty[ShipmentRequest]
    }
  }

  private def generateYear[T](yearOffset: Int, generateDayFxn : (LocalDate => Source[T, NotUsed])): Source[T, NotUsed] = {
    val year = lastDayToGenerate.getYear - yearOffset

    val firstDay = new LocalDate(year, 1, 1)
    val lastDayOfYear = new LocalDate(year, 12, 31)
    val lastDay = if (lastDayToGenerate.isBefore(lastDayOfYear)) lastDayToGenerate else lastDayOfYear

    val daysToGenerate = Days.daysBetween(firstDay, lastDay).getDays

    Source(0 until daysToGenerate).map { dayIndex =>
      generateDayFxn(firstDay.plusDays(dayIndex))
    }.flatMapConcat(identity)
  }
}
