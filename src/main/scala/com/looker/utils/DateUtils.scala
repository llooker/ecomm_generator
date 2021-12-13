package com.looker.utils

import breeze.stats.distributions.Gamma
import com.looker.generator.{UserData, Session}
import com.looker.random.WeightedDistribution
import org.joda.time.{DateTimeZone, DateTime, DateTimeConstants, LocalDate}

import scala.util.Random

object DateUtils {
  def blackFriday(year: Int): LocalDate = {
    getNthDayOfMonth(4, 11, year, DateTimeConstants.THURSDAY).plusDays(1)
  }

  def cyberMonday(year : Int): LocalDate = {
    blackFriday(year).plusDays(3)
  }

  def getNthDayOfMonth(n: Int, month : Int, year: Int, dayOfWeek: Int) = {
    var d = new LocalDate(year, month, 1).withDayOfWeek(dayOfWeek)
    if(d.getMonthOfYear != month) d = d.plusWeeks(1)
    if (n > 1) { d = d.plusWeeks(n - 1)}

    d
  }

  val eastern = Set(
    "Connecticut", "Delaware", "Florida", "Georgia", "Maine", "Maryland", "Massachusetts", "Michigan", "New Hampshire",
    "New Jersey", "New York", "North Carolina", "Ohio", "Pennsylvania", "Rhode Island", "South Carolina", "Vermont", "Virginia",
    "West Virginia"
  )

  val central = Set(
    "Alabama", "Arkansas", "Florida", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Michigan",
    "Minnesota", "Mississippi", "Missouri", "Nebraska", "North Dakota", "Oklahoma", "South Dakota", "Tennessee",
    "Texas", "Wisconsin"
  )

  val mountain = Set(
    "Colorado", "Idaho", "Montana", "New Mexico", "Utah", "Wyoming", "Arizona"
  )

  val pacific = Set(
    "California", "Nevada", "Oregon", "Washington"
  )

  val weekends = Set(DateTimeConstants.SATURDAY, DateTimeConstants.SUNDAY)
  val times          = Array(23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10,  9,  8,  7,  6,  5,  4,  3,  2,  1,  0)
  val weekdayWeights = Array(25, 32, 40, 47, 43, 40, 36, 43, 51, 68, 77, 85, 77, 69, 56, 44, 35, 27, 22, 18, 13,  9, 13, 22)
  val weekendWeights = Array(25, 32, 40, 45, 47, 55, 58, 68, 75, 83, 81, 74, 67, 60, 57, 47, 40, 32, 22, 21, 19, 12, 10, 15)
  val weekdayTimeDistribution = WeightedDistribution(times, weekdayWeights)
  val weekendTimeDistribution = WeightedDistribution(times, weekendWeights)

  def generateNextSessionDateLinear(current : LocalDate, maxDays: Int, offset : Int = 0): LocalDate = {
    val days = (maxDays - Math.sqrt(Random.nextDouble() * maxDays * maxDays)).toInt + offset + 1

    new LocalDate(current).plusDays(days)
  }

  def generateNextSessionDateLambda(current: LocalDate, mean: Int, dist: Int): LocalDate = {
    val days = Gamma(mean, dist).sample().toInt

    new LocalDate(current).plusDays(days)
  }

  // TODO push holiday deliverd/shipped
  def nearestWeekDayOffset(date: LocalDate, offset: Int = 0): Int = (date.getDayOfWeek + offset) match {
    case DateTimeConstants.SATURDAY => 1 + offset
    case DateTimeConstants.SUNDAY => 2 + offset
    case _ => + offset
  }

  def generateSessionStartTime(day: LocalDate, userData : UserData): DateTime = {
    val location = userData.location

    // TODO FIGURE OUT IF THIS IS RIGHT.
    val offset = {
      if (location.country == "UK") { 0 }
      else if (location.state == "Alaska") { 9 }
      else if (location.state == "Hawaii") { 10 }
      else if (pacific.contains(location.state)) { 8 }
      else if (central.contains(location.state)) { 7 }
      else if (mountain.contains(location.state)) { 6 }
      else if (eastern.contains(location.state)) { 5 }
      else { 4 }
    }

    val secondSundayMarch: LocalDate = DateUtils.getNthDayOfMonth(2, DateTimeConstants.MARCH, day.getYear, DateTimeConstants.SUNDAY)
    val firstSundayNov: LocalDate = DateUtils.getNthDayOfMonth(1, DateTimeConstants.NOVEMBER, day.getYear, DateTimeConstants.SUNDAY)

    var localTimeHour = if (weekends.contains(day.getDayOfWeek)) {
      weekendTimeDistribution.sample
    } else {
      weekdayTimeDistribution.sample
    }

    if (location.state != "Arizona") {
      if (day.isEqual(secondSundayMarch) && localTimeHour == 1) {
        localTimeHour += 1
      } else if (day.isEqual(firstSundayNov) && localTimeHour == 2) {
        localTimeHour -= 1
      }
    }
    val hour = (localTimeHour + offset + 24) % 24
    val minute = Random.nextInt(60)
    val second = Random.nextInt(60)

    new DateTime(day.getYear, day.getMonthOfYear, day.getDayOfMonth, hour, minute, second, DateTimeZone.UTC)
  }

}
