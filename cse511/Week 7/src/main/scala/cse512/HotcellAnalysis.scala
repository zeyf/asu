package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
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
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  var intersectedPickupInfo = pickupInfo.select(
    "x",
    "y",
    "z"
  ).where(
    "x >= " + minX
    + " AND x <= " + maxX
    + " AND y >= " + minY
    + " AND y <= " + maxY
    + " AND z >= " + minZ
    + " AND z <= " + maxZ
  )

  var cellHotnessDataframe = intersectedPickupInfo.groupBy(
      "z", 
      "y", 
      "x"
    ).count().withColumnRenamed(
      "count",
      "cell_hotness"
    ).orderBy(
      "z",
      "y",
      "x"
    )
  cellHotnessDataframe.createOrReplaceTempView("cellHotness")

  val average = (
    cellHotnessDataframe.select(
      "cell_hotness"
    ).agg(
      sum("cell_hotness")
    ).first().getLong(0).toDouble
  ) / numCells

  val standardDeviation = scala.math.sqrt(
    (
      cellHotnessDataframe.withColumn(
        "square_root",
        pow(
          col("cell_hotness"),
          2
        )
      ).select(
        "square_root"
      ).agg(
        sum("square_root")
      ).first().getDouble(0) / numCells
    )
    - scala.math.pow(
      average,
      2
    )
  )

  var nAdjCells = spark.sql(
    "SELECT h1.x AS x, h1.y AS y, h1.z AS z,"
    + "sum(h2.cell_hotness) AS cellNumber"
    + "FROM HotnessOfCells AS h1, HotnessOfCells AS h2"
    + "WHERE (h2.y = h1.y+1 OR h2.y = h1.y OR h2.y = h1.y-1)"
    + "AND (h2.x = h1.x+1 OR h2.x = h1.x OR h2.x = h1.x-1)"
    + "AND (h2.z = h1.z+1 OR h2.z = h1.z OR h2.z = h1.z-1)"
    + "GROUP BY h1.z, h1.y, h1.x "
    + "ORDER BY h1.z, h1.y, h1.x"
  )

  var adjNumCalculationUdf = udf(
    (
      x: Int,
      y: Int,
      z: Int,
      minX: Int,
      maxX: Int,
      minY: Int,
      maxY: Int,
      minZ: Int,
      maxZ: Int
    ) => HotcellUtils.calculateNumberOfAdjacentCells(
      x,
      y,
      z,
      minX,
      maxX,
      minY,
      maxY,
      minZ,
      maxZ
    )
  )
  var adjCellSumDataframe = nAdjCells.withColumn(
    "adjCellSum",
    adjNumCalculationUdf(
      col("x"),
      col("y"),
      col("z"),
      lit(minX),
      lit(maxX),
      lit(minY),
      lit(maxY),
      lit(minZ),
      lit(maxZ)
    )
  )

  var gScoreCalculationUdf = udf(
    (
      x: Int,
      y: Int,
      z: Int,
      cellLoc: Int,
      nCells: Int,
      adjCellSum: Int,
      average: Double,
      standardDeviation: Double
    ) => HotcellUtils.calculateGScore(
      x,
      y,
      z,
      cellLoc,
      nCells,
      adjCellSum,
      average,
      standardDeviation
    )
  )
  var gScoreValue = (
    adjCellSumDataframe
  )
  .withColumn(
    "gScoreResult",
    gScoreCalculationUdf(
      lit(numCells),
      col("x"),
      col("y"),
      col("z"),
      col("adjCellSum"),
      col("cellNumber"),
      lit(average),
      lit(standardDeviation)
    )
  ).orderBy(
    desc("gScoreResult")
  ).limit(50)
  gScoreValue.show()

  return gScoreValue.select(
    col("x"),
    col("y"),
    col("z")
  )
}
}
