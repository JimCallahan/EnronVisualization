package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Index2i, Scalar }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.PointFloatAttr

import scala.xml.XML

import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader }

object ConvertFinancialTermsApp {
  /** Top level method. */
  def main(args: Array[String]) {
    try {
      println("Converting Dictionary to XML...")
      val dict = FinancialDictionary.loadText(Path("./data/source/ndfinterms"))

      val path = Path("./data/xml/FinancialDictionary.xml")
      println("  Writing: " + path)
      val out = new BufferedWriter(new FileWriter(path.toFile))
      try {
        XML.write(out, dict.toXML, "UTF-8", false, null)
      }
      finally {
        out.close
      }

      println
      println("ALL DONE!")
    }
    catch {
      case ex =>
        println("Uncaught Exception: " + ex.getMessage + "\n" +
          "Stack Trace:\n" + ex.getStackTraceString)
    }
  }
}