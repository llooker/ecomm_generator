package com.looker.utils

import java.io.File

object FileUtils {
  def removeDataFiles(path:String = "data") = {
    for {
      files <- Option(new File(path).listFiles)
      file <- files if file.getName.endsWith(".csv")
    } file.delete()
  }
}
