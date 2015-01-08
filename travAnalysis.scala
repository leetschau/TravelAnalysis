import java.util.Date
import java.util.Calendar
import java.util.concurrent.TimeUnit

val SIG_DATA = "datamine/travel/data/drawdata_2014112*_cs.utf8_1.csv"
val LOC_DATA = "datamine/gdCgiNode.csv"
val TRAVEL_MAP_FILE = "traveller_201411"
val DICT_SEP = ","
val SIG_SEP = "\""

val START = "2014-12-22  00:00:00"
val END = "2014-12-24  00:00:00"
val STEP = 15 // unit: minute
val DATE_FORMAT = new java.text.SimpleDateFormat("yyyy-MM-dd  HH:mm:SS")

// 以下position指原始信令表被双引号分割后各项目所在的列下标位置
val SIG_USER_ID_POS = 1
val SIG_START_TIME_POS = 7
val SIG_CGI_POS = 15

// 信令表sig_map中各项位置
val USER_ID_POS = 0
val TIME_POS = 1
val CITY_ID_POS = 2
val CITY_NAME_POS = 3
val NODE_ID_POS = 4
val NODE_NAME_POS = 5
val LONGITUDE_POS = 6
val LATITUDE_POS = 7

val startCal = Calendar.getInstance()
val endCal = Calendar.getInstance() 
startCal.setTime(DATE_FORMAT.parse(START))
endCal.setTime(DATE_FORMAT.parse(END))
val startL = DATE_FORMAT.parse(START).getTime
val stepL = TimeUnit.MILLISECONDS.convert(STEP, TimeUnit.MINUTES)

val loc_dict = sc.textFile(LOC_DATA).map(_.split(DICT_SEP)).map(x => (x(0), x(1))).toArray.toMap

val raw_data = sc.textFile(SIG_DATA).map(_.split(SIG_SEP).toList)
val raw_sig_map = raw_data.filter(x => x.size > 17)

// 元素示例：List(460015426202824, 2014-12-22  08:30:00, 深圳, 深圳, 宝安国际机场, 宝安国际机场, 113.8563, 22.6086)
val sig_map = raw_sig_map.map(x => { val loc_info = loc_dict.getOrElse(x(SIG_CGI_POS), "")
                                     if (loc_info.size > 0) {
                                       List(x(SIG_USER_ID_POS), x(SIG_START_TIME_POS),
                                           loc_info.split(" ")(0), loc_info.split(" ")(1),
                                           loc_info.split(" ")(2), loc_info.split(" ")(3),
                                           loc_info.split(" ")(4), loc_info.split(" ")(5))
                                     } else { List() }
                                   }).filter(_.size > 0)

def get_frame_start(time_pnt: String): String = {
  val curL = DATE_FORMAT.parse(time_pnt).getTime
  val offset = (curL - startL) % stepL
  val frame_start = new Date()
  frame_start.setTime(curL - offset)
  return DATE_FORMAT.format(frame_start.getTime)
}

def valid_time(inp_time: String): Boolean = {
    val inp_cal = Calendar.getInstance()
    inp_cal.setTime(DATE_FORMAT.parse(inp_time))
    if(inp_cal.after(startCal) && inp_cal.before(endCal)) true else false
}

// 时间窗口内
val time_win = sig_map.filter(x => valid_time(x(1)))

// 时间分窗，剔重
val split_win = time_win.map(x => List(x(0), get_frame_start(x(1))) ++ x.drop(2)).distinct

// 去掉只出现一次的用户
val all_users = split_win.groupBy(_(0))
val travel_users = all_users.filter(x => x._2.size > 1)

// 输入起点和终点的经纬度，返回两点间距离
def calc_distance(start_lat: String, start_lng: String, end_lat: String, end_lng: String): Long = {
  val AVERAGE_RADIUS_OF_EARTH = 6371

  val userLat = start_lat.toDouble
  val userLng = start_lng.toDouble
  val venueLat = end_lat.toDouble
  val venueLng = end_lng.toDouble

  val latDistance = Math.toRadians(userLat - venueLat)
  val lngDistance = Math.toRadians(userLng - venueLng)

  val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
        Math.cos(Math.toRadians(userLat)) * Math.cos(Math.toRadians(venueLat)) *
        Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2)

  val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

  return Math.round(AVERAGE_RADIUS_OF_EARTH * c)
}

// 输入起点和终点时间，返回二者时间差，单位：小时
def calc_duration(start: String, end: String): Double = {
  val startL = DATE_FORMAT.parse(start).getTime
  val endL = DATE_FORMAT.parse(end).getTime
  val duration = TimeUnit.MINUTES.convert(endL - startL, TimeUnit.MILLISECONDS)
  return duration / 60.0
}

// 计算一个用户在不同交通枢纽间的运动速度
def calc_travel_speed(travellist: List[List[String]]):  List[List[String]] = {
  val travel_sorted = travellist.sortBy(x => x(TIME_POS))
  var node_pairs = List(List(""))
  for ( x <- (0 until travel_sorted.size - 1)) {
    if ((travel_sorted(x)(NODE_ID_POS) != travel_sorted(x+1)(NODE_ID_POS)) &&
                  (travel_sorted(x)(TIME_POS) != travel_sorted(x+1)(TIME_POS))) {
      val start = travel_sorted(x)
      val end = travel_sorted(x+1)
      val distance = calc_distance(start(LATITUDE_POS), start(LONGITUDE_POS), end(LATITUDE_POS), end(LONGITUDE_POS))
      val duration = calc_duration(start(TIME_POS), end(TIME_POS))
      node_pairs = node_pairs :+ List(start(USER_ID_POS), start(TIME_POS), start(CITY_ID_POS), start(CITY_NAME_POS),
          start(NODE_ID_POS), start(NODE_NAME_POS), end(TIME_POS), end(CITY_ID_POS), end(CITY_NAME_POS),
          end(NODE_ID_POS), end(NODE_NAME_POS), (distance / duration).toString)
    }
  }
  return node_pairs.drop(1)
}

// 生成每个用户的移动路线：以一个用户为单位，包含他所有的移动路线
// RDD[List[String]]
// 元素示例：List(460012720617764, 2014-12-22  09:15:00, 珠海, 珠海, 珠海火车站, 珠海火车站, 2014-12-22  09:30:00, 珠海, 珠海, 珠海北站, 珠海北站, 32.052239154078805)
val travel_map = travel_users.map(x => x._2.toList).flatMap(x => calc_travel_speed(x))

travel_map.map(x => x.reduce(_ + "," + _)).saveAsTextFile(TRAVEL_MAP_FILE)
