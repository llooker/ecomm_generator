package com.looker.generator

import com.looker.random.WeightedDistribution

import scala.io.Source
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.util.Random

case class UserLocation(ip: String, city: String, state: String, country: String, lat: Double, long: Double, zip: String)

object LocationManager {

  case class SpecificLocation(zip: String, lat: Double, lon: Double, ip_addresses: Array[String]) {
    val length = ip_addresses.length

    def pickIPAddress() : String = {
      val ip = ip_addresses(Random.nextInt(length))
      var ip_array = ip.split("\\.")
      if (ip_array(ip_array.length - 1) == "0") {
        ip_array(ip_array.length - 1) = Random.nextInt(256).toString
      }
      ip_array.mkString(".")
    }
  }

  object SpecificLocation {
    implicit val reader = (
      (__ \ "zip").read[String] ~
        (__ \ "lat").read[Double] ~
        (__ \ "lon").read[Double] ~
        (__ \ "ip_addresses").read[Array[String]]
      )(SpecificLocation.apply _)
  }

  case class LocationData(city: String, state: String, country: String, weight: Int, locations: Array[SpecificLocation])

  object LocationData {
    implicit val reader = (
      (__ \ "city").read[String] ~
        (__ \ "state").read[String] ~
        (__ \ "country").read[String] ~
        (__ \ "weight").read[Int] ~
        (__ \ "locations").read[Array[SpecificLocation]]
      )(LocationData.apply _)
  }

  def initializeLocationData(source: Source): WeightedDistribution[LocationData, Int] = {
    val locationData = Json.parse(source.mkString).as[Array[LocationData]]
    val weights = locationData.map(data => data.weight)
    WeightedDistribution(locationData, weights)
  }

  val usSource = Source.fromURL(Source.getClass.getResource("/location_data.json"))
  val usWeightedData = LocationManager.initializeLocationData(usSource)

  val ukSource = Source.fromURL(Source.getClass.getResource("/uk_location_data.json"))
  val ukWeightedData = LocationManager.initializeLocationData(ukSource)

  val UkProbabilityBase = 0.167362924

  def pickLocation(): UserLocation = {
    val weightedLocations = if (Random.nextDouble < UkProbabilityBase) ukWeightedData else usWeightedData

    val loc = weightedLocations.sample
    val locationLength = loc.locations.length
    val location = loc.locations(Random.nextInt(locationLength))

    val randomized_lat_lng_range = if (locationLength > 1) { 4 } else { 1 }

    val km_per_degree_approx = 111.1
    val range_deg = 1 / (km_per_degree_approx / randomized_lat_lng_range)

    val lat = location.lat + (2 * range_deg) * Random.nextDouble - range_deg
    val lon = location.lon + (2 * range_deg) * Random.nextDouble - range_deg

    new UserLocation(location.pickIPAddress(), loc.city, loc.state, loc.country, lat, lon, location.zip)
  }
}


