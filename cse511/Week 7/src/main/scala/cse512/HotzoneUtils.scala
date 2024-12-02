package cse512

object HotzoneUtils {
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val rectCoords = queryRectangle.split(",")
    val rect_x1 = rectCoords(0).trim.toDouble
    val rect_y1 = rectCoords(1).trim.toDouble
    val rect_x2 = rectCoords(2).trim.toDouble
    val rect_y2 = rectCoords(3).trim.toDouble
    val pointCoords = pointString.split(",")
    val point_x = pointCoords(0).trim.toDouble
    val point_y = pointCoords(1).trim.toDouble
    val lower_x = if (rect_x1 < rect_x2) rect_x1 else rect_x2
    val higher_x = if (rect_x1 > rect_x2) rect_x1 else rect_x2
    val lower_y = if (rect_y1 < rect_y2) rect_y1 else rect_y2
    val higher_y = if (rect_y1 > rect_y2) rect_y1 else rect_y2


    point_x >= lower_x && point_x <= higher_x && point_y >= lower_y && point_y <= higher_y
  }
}
