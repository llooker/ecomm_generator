package com.looker.utils

import org.joda.time.format.DateTimeFormat
import org.joda.time.{ReadablePartial, ReadableInstant, DateTime, LocalDate}

object TimeFormatter {

  val timeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  def format(time: DateTime) : String = timeFormatter.print(time)

  def format(date: LocalDate) : String = dateFormatter.print(date)

  def formatNullable(date : Option[Any])(implicit nullString : String) : String = date match {
    case Some(d : DateTime)  => format(d)
    case Some(d : LocalDate) => format(d)
    case _                   => nullString
  }
}
