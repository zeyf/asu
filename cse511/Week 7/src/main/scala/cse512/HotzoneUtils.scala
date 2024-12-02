package cse512

object HotzoneUtils {

  def ST_Contains(
    queryRectangle: String,
    pointString: String
  ): Boolean = {
    // Validate the input if we are to actually calculate an intersection
    if (isValidInput(queryRectangle, pointString)) {
      // Parse out the coordinates
      val rCoords = queryRectangle.split(",")
      val pCoords = pointString.split(",")
      val pX = pCoords(0).trim.toDouble
      val pY = pCoords(1).trim.toDouble
      val cX1 = rCoords(0).trim.toDouble
      val cY1 = rCoords(1).trim.toDouble
      val cX2 = rCoords(2).trim.toDouble
      val cY2 = rCoords(3).trim.toDouble

      // Verify there if there is not a full intersection (the point is within the rectangle)
      if (pX < math.min(cX1, cX2)) {
        return false
      } else if (pX > math.max(cX1, cX2)) {
        return false
      } else if (pY < math.min(cY1, cY2)) {
        return false
      } else if (pY > math.max(cY1, cY2)) {
        return false
      }

      // The point is within the rectangle, we have an intersection
      return true
    }

    // We have invalid input
    return false
  }

  def isValidInput(
    queryRectangle: String,
    pointString: String
  ): Boolean = {
    // Validate the string has something
    if (
      queryRectangle.length == 0
      || pointString.length == 0
    ) {
      return false
    }

    // Validate we have the coordinates of the two corners of the rectangle
    val rCoords = queryRectangle.split(",")
    if (rCoords.length != 4) {
      return false
    }

    // Validate we have the coordinateas of the point
    val pCoords = pointString.split(",")
    if (pCoords.length != 2) {
      return false
    }

    return true
  }
}
