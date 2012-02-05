package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Vec2d, Index2i, Frame2d, Scalar, Interval }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PointFloatAttr, PrimitiveFloatAttr }

import scala.xml.{ PrettyPrinter, XML }
import scala.collection.immutable.{ TreeSet, TreeMap }
import scala.collection.mutable.{ HashMap, HashSet }
import scala.math.{ E, pow, log }

import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader, IOException }

import scala.xml.{ Node, XML }

trait CommonIO {
  //-----------------------------------------------------------------------------------------------------------------------------------
  //   X M L   I N P U T 
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Write the list of validated people, returning central ones only. */
  def readPeopleXML(central: TreeSet[PersonalCentrality]): TreeMap[Long, Person] = {
    val path = Path("./data/xml/people.xml")
    println("  Reading: " + path)
    val in = new BufferedReader(new FileReader(path.toFile))
    try {
      val xml = XML.load(in)
      val centralIDs = central.map(_.pid)
      var people = TreeMap[Long, Person]()
      for (pc <- xml \\ "Person") {
        val person = Person.fromXML(pc)
        if (centralIDs.contains(person.pid))
          people = people + (person.pid -> person)
      }
      people
    }
    finally {
      in.close
    }
  }

  /** Read the most central (eigenvector centrality) people. */
  def readMostCentralXML: TreeSet[PersonalCentrality] = {
    val path = Path("./data/xml/mostCentral.xml")
    println("  Reading: " + path)
    val in = new BufferedReader(new FileReader(path.toFile))
    try {
      val xml = XML.load(in)
      var central = TreeSet[PersonalCentrality]()
      for (pc <- xml \\ "PersonalCentrality")
        central = central + PersonalCentrality.fromXML(pc)
      central
    }
    finally {
      in.close
    }
  }

  /** Read in edges and associated attributes.
    * @return (Time Stamp, Interval, Samples)
    */
  def readBundleSentimentSamplesXML(xmldir: Path, prefix: String, frame: Int): (Long, Long, HashMap[Long, HashMap[Long, AverageBiSentiment]]) = {
    val path = xmldir + Path(prefix) + (prefix + ".%04d.xml".format(frame))
    println("Reading XML File: " + path)
    val in = new BufferedReader(new FileReader(path.toFile))
    try {
      val xml = XML.load(in)

      val xframe = (xml \\ "Frame").text.toInt
      if (frame != xframe) throw new IOException("Wrong input frame!")
      else {
        val stamp = (xml \\ "TimeStamp").text.toLong
        val interval = (xml \\ "Interval").text.toLong
        val rtn = HashMap[Long, HashMap[Long, AverageBiSentiment]]()
        for (s <- xml \\ "AvgBiSent") {
          val snt = AverageBiSentiment.fromXML(s)
          val sm = rtn.getOrElseUpdate(snt.sendID, HashMap[Long, AverageBiSentiment]())
          sm += (snt.recvID -> snt)
        }

        (stamp, interval, rtn)
      }
    }
    finally {
      in.close
    }
  }

  /** Read the bundled edges and associated attributes and personal IDs. */
  def readBundlerAttrsXML(outdir: Path,
                         prefix: String,
                         frame: Int): (Bundler, Array[AttrIndex]) = {

    val path = outdir + (prefix + ".%04d.xml".format(frame))
    println("Reading XML File: " + path)
     val in = new BufferedReader(new FileReader(path.toFile))
    try {
      val xml = XML.load(in)
      val bundler = Bundler.fromXML((xml \\ "Bundler").head)
      val attrs = for (ai <- xml \\ "AttrIndices" \\ "AttrIndex") yield AttrIndex.fromXML(ai)
      (bundler, attrs.toArray)
    }
    finally {
      in.close
    }
  }

  //-----------------------------------------------------------------------------------------------------------------------------------
  //   X M L   O U T P U T 
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Write the list of validated people. */
  def writePeopleXML(ps: TreeMap[Long, Person]) {
    val path = Path("./data/xml/people.xml")
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val pp = new PrettyPrinter(100, 2)
      out.write(pp.format(<People>{ ps.values.map(_.toXML) }</People>))
    }
    finally {
      out.close
    }
  }

  /** Write the most central (eigenvector centrality) people. */
  def writeMostCentralXML(central: TreeSet[PersonalCentrality]) {
    val path = Path("./data/xml/mostCentral.xml")
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val pp = new PrettyPrinter(100, 2)
      out.write(pp.format(<PersonalCentralities>{ central.map(_.toXML) }</PersonalCentralities>))
    }
    finally {
      out.close
    }
  }

  /** Write the bundled edges and associated attributes and personal IDs. */
  def writeBundlerAttrsXML(outdir: Path,
                           prefix: String,
                           frame: Int,
                           bundler: Bundler,
                           attrIndices: Array[AttrIndex]) {

    val path = outdir + (prefix + ".%04d.xml".format(frame))
    println("Writing XML File: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val xml =
        <BundlerAttrs>{ bundler.toXML }<AttrIndices>{
          attrIndices.map(_.toXML)
        }</AttrIndices></BundlerAttrs>

      val pp = new PrettyPrinter(100, 2)
      out.write(pp.format(xml))
    }
    finally {
      out.close
    }
  }

}