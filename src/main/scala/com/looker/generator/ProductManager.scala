package com.looker.generator

import java.security.MessageDigest

import breeze.stats.distributions.Gamma
import com.looker.random.WeightedDistribution

import scala.collection.immutable.HashMap
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.io.Source
import scala.util.Random

case class DistributionCenter(id: Int, name : String, lat : Double, long : Double)

case class Product(name: String, id: Int, retailPrice: Double, cost: Double, sku: String, brand: Brand, distributionCenter : DistributionCenter, rank : Int) {
  def category = brand.category
}
case class Brand(name: String, category: Category)

case class Category(name: String, department: Department.Value) {
  override def toString = name + " " + department.toString
}

object Department extends Enumeration {
  val Men, Women = Value

  def name(department : Value) = department.toString
}

object ProductManager {

  val distributionCenters = Array(
    DistributionCenter(1, "Memphis TN", 35.1174, -89.9711), // "Memphis, TN",
    DistributionCenter(2, "Chicago IL", 41.8369, -87.6847), //"Chicago, IL",
    DistributionCenter(3, "Houston TX", 29.7604, -95.3698), // "Houston, TX",
    DistributionCenter(4, "Los Angeles CA", 34.0500, -118.250), // "Los Angeles, CA"
    DistributionCenter(5, "New Orleans LA", 29.9500, -90.0667), // "New Orleans, LA",
    DistributionCenter(6, "Port Authority of New York/New Jersey NY/NJ", 40.6340, -73.7834), // "Port Authority of New York/New Jersey, NY/NJ",
    DistributionCenter(7, "Philadelphia PA", 39.9500, -75.1667), // "Philadelphia, PA",
    DistributionCenter(8, "Mobile AL", 30.6944, -88.0431), // "Mobile, AL"
    DistributionCenter(9, "Charleston SC", 32.7833, -79.9333), // "Charleston, SC",
    DistributionCenter(10, "Savannah GA", 32.0167, -81.1167)  // "Savannah, GA"
  )
  val distCenterMap = HashMap( distributionCenters.map(e => (e.id, e)).toSeq : _* )

  val distributionCenterDistribution = WeightedDistribution(distributionCenters, Array(15, 13, 12, 8, 8, 8, 7, 7, 6, 6))

  val maleUnbiasedDeparmentDist      = WeightedDistribution(Array(Department.Women, Department.Men), Array(12, 88))
  val femaleUnbiasedDeparmentDist    = WeightedDistribution(Array(Department.Women, Department.Men), Array(73, 27))

  val maleMenBiasedDeparmentDist     = WeightedDistribution(Array(Department.Women, Department.Men), Array( 3, 97))
  val femaleMenBiasedDeparmentDist   = WeightedDistribution(Array(Department.Women, Department.Men), Array(40, 60))
  val maleWomenBiasedDeparmentDist   = WeightedDistribution(Array(Department.Women, Department.Men), Array(30, 79))
  val femaleWomenBiasedDeparmentDist = WeightedDistribution(Array(Department.Women, Department.Men), Array( 1, 99))

  def generateBiasedDepartment(userData : UserData, bias : Department.Value) : Department.Value = {
    if (bias == Department.Men && userData.gender == Gender.Male) {
      maleMenBiasedDeparmentDist.sample
    } else if (bias == Department.Men && userData.gender == Gender.Female) {
      femaleMenBiasedDeparmentDist.sample
    } else if (bias == Department.Women && userData.gender == Gender.Male){
      maleWomenBiasedDeparmentDist.sample
    } else {
      femaleWomenBiasedDeparmentDist.sample
    }
  }

  def generateDepartment(userData: Option[UserData]) : Department.Value = {
    if ((userData.isEmpty && Random.nextDouble < 0.63) || (userData.nonEmpty && userData.get.gender == Gender.Male)) {
      maleUnbiasedDeparmentDist.sample
    } else {
      femaleUnbiasedDeparmentDist.sample
    }
  }

  val allMenCategories = Array(
    "Accessories", "Active", "Fashion Hoodies & Sweatshirts", "Jeans", "Outerwear & Coats", "Pants",
    "Shorts", "Sleep & Lounge", "Socks", "Suits & Sport Coats", "Sweaters", "Swim", "Tops & Tees", "Underwear"
  )

  val allMenCategoryProfit = Array(10, 8, -5, -3, 6, 4, 0, 10, -10, 10, 0, -10, -6, 3)

  val menCategoryDistribution = WeightedDistribution(allMenCategories, Array(12, 10, 7, 13, 6, 10, 10, 4, 3, 3, 4, 2, 10, 3))

  val menCategoryProfit = HashMap(allMenCategories.zip(allMenCategoryProfit) : _*)

  val allWomenCategories = Array(
    "Accessories", "Active", "Blazers & Jackets", "Clothing Sets", "Dresses", "Fashion Hoodies & Sweatshirts",
    "Intimates", "Jeans", "Jumpsuits & Rompers", "Leggings", "Maternity", "Outerwear & Coats", "Pants & Capris",
    "Plus", "Shorts", "Skirts", "Sleep & Lounge", "Socks & Hosiery", "Suits", "Sweaters", "Swim", "Tops & Tees"
  )

  val allWomenCategoryProfit = Array(10, 8, 12, -12, 5, 3, -3, -4, -3, -10, 6, 5, -3, 0, 0, 10, -10, 10, -10, 5,  8, -6)

  val womenCategoryProfit = HashMap(allWomenCategories.zip(allWomenCategoryProfit) : _*)

  val womenCategoryDistribution = WeightedDistribution(allWomenCategories, Array(15, 8, 6, 3, 12, 2, 4, 12, 3, 4, 2, 4, 4, 3, 5, 8, 2, 3, 2, 5, 6, 12))

  def generateCategory(userData: Option[UserData]): Category =  {
    generateCategoryFromDepartment(userData, generateDepartment(userData))
  }

  def generateCategoryFromDepartment(userData: Option[UserData], department: Department.Value): Category = {
    val cat = if (department == Department.Men) {
      menCategoryDistribution.sample
    } else {
      womenCategoryDistribution.sample
    }
    Category(cat, department)
  }

  def generateBiasedCategory(userData: Option[UserData], bias: Category) = {
    val rand = Random.nextDouble
    if (rand < 0.341) {
      bias
    } else if (rand < 0.543) {
      generateCategoryFromDepartment(userData, bias.department)
    } else {
      generateCategory(userData)
    }
  }

  val products: Map[Category, WeightedDistribution[Product, Int]] = populateProductData

  def generateProductInCategory(category: Category): Product = {
    products.get(category).get.sample
  }

  def generateProduct(userData: Option[UserData]) : Product = {
    generateProductInCategory(generateCategory(userData))
  }

  // TODO: do this -- just copy from old generator
  def generateCost(retailPrice: Double, category: Category) : Double = {
    val lookup = if (category.department == Department.Men) {
      menCategoryProfit
    } else {
      womenCategoryProfit
    }

    val offset: Int = lookup.get(category.name).get

    val mult = 0.5 + (Random.nextInt(100) - 49).toFloat / 1000 - offset.toFloat / 100

    retailPrice * mult
  }

  def generateAgeBeforeSale(product : Product) : Int = {
    Gamma(4, 7).sample().toInt
  }

  def newReadProducts : Iterator[Product] = {
    val bufferedSource = Source.fromURL(Source.getClass().getResource("/products.csv"))
    bufferedSource.getLines.map { line =>
      val cols = line.split(",").map(_.trim)

      val id          = cols(0).toInt
      val cost        = cols(1).toDouble
      val category    = cols(2)
      val name        = cols(3)
      val brand       = cols(4)
      val retailPrice = cols(5).toFloat
      val department  = cols(6)
      val sku         = cols(7)
      val centerId    = cols(8).toInt
      val rank        = cols(9).toInt

      Product(name, id, retailPrice, cost, sku, Brand(brand, Category(category, Department.withName(department))), distCenterMap.get(centerId).get, rank)
    }
  }

  private def populateProductData: HashMap[Category, WeightedDistribution[Product, Int]] = {

    val temporary = MutableHashMap[Category, (Array[Product], Array[Int])]()

    newReadProducts.foreach(product => {
      if (temporary.contains(product.category)) {
        val pair: (Array[Product], Array[Int]) = temporary.get(product.category).get
        temporary.put(product.category, (pair._1 :+ product, pair._2 :+ product.rank))
      } else {
        temporary.put(product.category, (Array(product), Array(product.rank)))
      }
    })

    // construct the weighted distributions
    val seq = temporary.map { case (cat, pair) => (cat, WeightedDistribution(pair._1, pair._2))}.toSeq
    // construct a mutable HashMap rather than immutable
    HashMap( seq : _* )
  }
}
