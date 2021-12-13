package com.looker.generator

import akka.actor.{Actor, ActorLogging}
import com.looker.random.WeightedDistribution
import org.joda.time.{DateTimeConstants, DateTime, LocalDate}

import scala.concurrent.Future
import scala.io.Source
import scala.util.Random
import scala.util.matching.Regex

case class UserData(location: UserLocation, os: String, browser: String, gender: Gender.Value, age: Int, trafficSource: String, optionalId: Option[Future[Int]] = None, orderCount: Int = 0, loggedIn : Boolean = false) {
  def withId(id: Future[Int]) = { copy(optionalId = Some(id)) }
  def incrementOrderCount = { copy(orderCount = orderCount + 1) }
  def login  = { copy(loggedIn = true) }
  def logout = { copy(loggedIn = false) }
}

object Gender extends Enumeration {
  val Male, Female = Value
}

object UserManager {
  def nameDistribution(file: String) = {
    val bufferedSource = Source.fromURL(Source.getClass.getResource(file))
    var names: List[String] = List()
    var values: List[Int] = List()

    val pattern = """(\w*)\s*(\S*).*""".r
    for (line <- bufferedSource.getLines) {
      val pattern(name, score) = line
      names = name :: names
      values = (score.toDouble * 1000).toInt :: values
    }
    WeightedDistribution(names.toArray, values.toArray)
  }

  val maleFirstNameDistribution   = nameDistribution("/dist.male.first")
  val femaleFirstNameDistribution = nameDistribution("/dist.female.first")
  val lastNameDistribution        = nameDistribution("/dist.all.last")

  def getName(gender: Gender.Value) : (String, String) = {
    val firstName = if (gender == Gender.Male){
      maleFirstNameDistribution.sample
    } else {
      femaleFirstNameDistribution.sample
    }

    val unformattedLastName = lastNameDistribution.sample
    val lastName = "\\w*".r.replaceAllIn(unformattedLastName, _.group(0).capitalize)

    (firstName.capitalize, lastName)
  }

  val domainDistribution = WeightedDistribution(
    Array("gmail.com", "yahoo.com", "aol.com", "hotmail.com"),
    Array(64, 32, 7, 3)
  )

  def getEmail(firstName: String, lastName : String): String = {
    val first = if(Random.nextDouble() > 0.3) {
      if (Random.nextDouble > 0.9) {
        firstName + "."
      } else {
        firstName
      }
    } else {
      firstName.charAt(0).toString
    }

    (first + lastName + "@" + domainDistribution.sample).toLowerCase
  }

  /* Browser Distributions */
  val browsers = Array("Chrome", "Safari", "IE", "Firefox", "Other")
  val macBrowserDistribution     = WeightedDistribution(browsers, Array(35, 56, 0, 9, 2))
  val windowsBrowserDistribution = WeightedDistribution(browsers, Array(60, 4, 23, 17, 2))
  val linuxBrowserDistribution   = WeightedDistribution(browsers, Array(68, 0, 0, 28, 4))

  def getBrowser(os: String): String = {
    if (os == "Macintosh") {
      macBrowserDistribution.sample
    } else if (os == "Windows") {
      windowsBrowserDistribution.sample
    } else {
      linuxBrowserDistribution.sample
    }
  }

  val osDistribution = WeightedDistribution(Array("Macintosh", "Windows", "Linux"), Array(48, 41, 11))

  def getOS : String = {
    osDistribution.sample
  }

  def getGender(os: String): Gender.Value = {
    val probabilityFemale = os match {
      case "Macintosh" => 0.58
      case "Windows" => 0.55
      case "Linux" => 0.47
    }

    if (Random.nextDouble() <= probabilityFemale) {
      Gender.Female
    } else {
      Gender.Male
    }
  }

  val ages = Array((12, 13), (14, 15), (16, 17), (18, 35), (36, 40), (41, 45), (46, 50), (51, 55), (56, 60), (61, 65), (66, 75), (76, 90), (91, 102))
  val weekdayAgeDistribution = WeightedDistribution(ages, Array(60, 70, 80, 100, 95, 88, 78, 65, 52, 42, 35, 20, 10))
  val weekendAgeDistribution = WeightedDistribution(ages, Array(60, 70, 80, 93, 100, 78, 65, 55, 49, 45, 30, 20, 10))

  def getAge(day: LocalDate): Int = {
    val range = if (day.getDayOfWeek == DateTimeConstants.SATURDAY || day.getDayOfWeek == DateTimeConstants.SUNDAY) {
      weekendAgeDistribution.sample
    } else {
      weekdayAgeDistribution.sample
    }

    Random.nextInt(range._2 - range._1) + range._1
  }

  val trafficSourceDistribution = WeightedDistribution(
    Array("Organic", "Email", "Search", "Facebook", "Display"),
    Array(42, 8, 20, 12, 9)
  )

  val cancelTrafficSourceDistribution = WeightedDistribution(
    Array("Organic", "Email", "Search"), Array(10, 60, 30)
  )
  def getTrafficSource(day: LocalDate, age: Int, cancel: Boolean = false): String = {
    // TODO more complicated metric about time of day / day of year
    // incorporate age
    if (cancel) {
      cancelTrafficSourceDistribution.sample
    } else {
      trafficSourceDistribution.sample
    }
  }

  def generateUserData(day: LocalDate, location : UserLocation) : UserData = {
    val os = UserManager.getOS
    val browser = UserManager.getBrowser(os)
    val gender = UserManager.getGender(os)
    val age = UserManager.getAge(day)

    // TODO: time of day
    val trafficSource = UserManager.getTrafficSource(day, age)

    UserData(location, os, browser, gender, age, trafficSource)
  }
}
