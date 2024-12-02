package cse512

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
    {
      // Load the original data from a data source
      var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
      pickupInfo.createOrReplaceTempView("nyctaxitrips")
      //pickupInfo.show()
      // Assign cell coordinates based on pickup points
      spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0))))
      spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1))))
      spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2))))
      pickupInfo = spark.sql("SELECT CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
      var newCoordinateName = Seq("x", "y", "z")
      pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
      pickupInfo.createOrReplaceTempView("pickupinfo")
      //pickupInfo.show()
      //print(pickupInfo.count())

      // Define the min and max of x, y, z
      val minX = -74.50 / HotcellUtils.coordinateStep
      val maxX = -73.70 / HotcellUtils.coordinateStep
      val minY = 40.50 / HotcellUtils.coordinateStep
      val maxY = 40.90 / HotcellUtils.coordinateStep
      val minZ = 1
      val maxZ = 31
      val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

      val pointsWithinIntersectingScope = spark.sql(
        "SELECT x,y,z FROM pickupinfo WHERE "
        + "x >= " + minX + " AND x <= " + maxX
        + " AND y >= " + minY + " AND y <= " + maxY
        + " AND z >= " + minZ + " AND z <= " + maxZ
        + " ORDER BY z, y, x"
      ).persist();
      pointsWithinIntersectingScope.createOrReplaceTempView("Df0")

      val pointsWithinIntersectingScope1 = spark.sql(
        """
          SELECT
            x,
            y,
            z,
            COUNT(*) AS pointval
          FROM Df0
          GROUP BY
            z,
            y,
            x
          ORDER BY
            z,
            y,
            x
        """
      ).persist();
      pointsWithinIntersectingScope1.createOrReplaceTempView("Df1")

      spark.udf.register(
        "squared",
        (x: Int) => (
          (
            HotcellUtils.squared(x)
          )
        )
      )

      val pointSum = spark.sql(
        """
        SELECT
          COUNT(*) AS countval,
          SUM(pointval) AS sumval,
          SUM(squared(pointval)) AS squaredsum from Df1
        """
      )
      pointSum.createOrReplaceTempView("pointSum")

      val pointsS0 = pointSum.first().getLong(1);
      val pointsS1 = pointSum.first().getDouble(2);

      val average = (
        pointsS0.toDouble / numCells.toDouble
      ).toDouble;
      val SD = math.sqrt(
        (
          (pointsS1.toDouble / numCells.toDouble)
          - (average.toDouble * average.toDouble)
        )
      ).toDouble

      spark.udf.register("NeighbourCount", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, inputX: Int, inputY: Int, inputZ: Int)
      => ((HotcellUtils.CountNeighbours(minX, minY, minZ, maxX, maxY, maxZ, inputX, inputY, inputZ))))
      val nbrs = spark.sql("SELECT NeighbourCount("+minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "a1.x,a1.y,a1.z) AS nCount, COUNT(*) AS countall, a1.x AS x,a1.y AS y,a1.z AS z, sum(a2.pointval) AS sumtotal from Df1 AS a1, Df1 AS a2 where (a2.x = a1.x+1 or a2.x = a1.x or a2.x = a1.x-1) and (a2.y = a1.y+1 or a2.y = a1.y or a2.y =a1.y-1) and (a2.z = a1.z+1 or a2.z = a1.z or a2.z =a1.z-1) group by a1.z,a1.y,a1.x order by a1.z,a1.y,a1.x").persist()
      nbrs.createOrReplaceTempView("Df2");

      spark.udf.register("GScore", (x: Int, y: Int, z: Int, average:Double, sd: Double, countn: Int, sumn: Int, numcells: Int) => ((
      HotcellUtils.calcGScore(x, y, z, average, sd, countn, sumn, numcells))))
      
      val nbrs1 = spark.sql("SELECT GScore(x,y,z,"+average+","+SD+",ncount,sumtotal,"+numCells+") AS gtstat,x, y, z FROM Df2 ORDER BY gtstat DESC");
      nbrs1.createOrReplaceTempView("Df3")
      
      val result = spark.sql("SELECT x,y,z from Df3")
      result.createOrReplaceTempView("Df4")
      return result 
    }
}
