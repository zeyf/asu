package cse512

object HotzoneUtils {
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    // Split and parse the query rectangle coordinates
    val Array(rect_x1, rect_y1, rect_x2, rect_y2) = queryRectangle.split(",").map(_.trim.toDouble)

    // Split and parse the point coordinates
    val Array(point_x, point_y) = pointString.split(",").map(_.trim.toDouble)

    // Determine the lower and higher x, and lower and higher y values for the rectangle
    val (lower_x, higher_x) = if (rect_x1 < rect_x2) (rect_x1, rect_x2) else (rect_x2, rect_x1)
    val lower_y = math.min(rect_y1, rect_y2)
    val higher_y = math.max(rect_y1, rect_y2)

    // Check if the point lies within the rectangle bounds
    point_x >= lower_x && point_x <= higher_x && point_y >= lower_y && point_y <= higher_y
  }
}
