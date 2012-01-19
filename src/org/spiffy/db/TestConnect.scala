package org.spiffy.db

import java.sql.{ Connection, DriverManager, ResultSet }

object TestConnect {
  // Loads the JDBC driver. 
  classOf[com.mysql.jdbc.Driver]

  def main(args: Array[String]) {
    try {
      val connStr = "jdbc:mysql://localhost:3306/enron?user=enron&password=slimyfucks"
      val conn = DriverManager.getConnection(connStr)
      try {
        // Configure to be Read Only
        val statement = conn.createStatement()

        // Execute Query
        val rs = statement.executeQuery("SELECT body FROM bodies LIMIT 3")

        // Iterate Over ResultSet
        while (rs.next) {
          println(rs.getString("body"))
        }
      } 
      finally {
        conn.close
      }
    } 
    catch {
      case ex =>
        println("Uncaught Exception: " + ex.getMessage + "\n" +
          "Stack Trace:\n" + ex.getStackTraceString)
    }
  }
}