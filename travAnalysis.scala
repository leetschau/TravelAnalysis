import java.util.Date
import java.util.Calendar
import java.util.concurrent.TimeUnit

val SIG_DATA = "datamine/travel/data/drawdata_2014112*_cs.utf8_1.csv"
val LOC_DATA = "datamine/gdCgiNode.csv"
val USER_MAP_FILE = "traveller_201411"
val DICT_SEP = ","
val SIG_SEP = "\""

val START = "2014-12-22  00:00:00"
val END = "2014-12-24  00:00:00"
val STEP = 15 // unit: minute
val DATE_FORMAT = new java.text.SimpleDateFormat("yyyy-MM-dd  HH:mm:SS")

// 以下position指原始信令表被双引号分割后各项目所在的列下标位置
val START_TIME_POS = 7
val CGI_POS = 15
val USER_ID_POS = 1

// 以下position指sig_map中的下标位置
val TIME_POS = 1
val CITY_ID_POS = 3
val TYPE_POS = 6

val startCal = Calendar.getInstance()
val endCal = Calendar.getInstance() 
startCal.setTime(DATE_FORMAT.parse(START))
endCal.setTime(DATE_FORMAT.parse(END))
val startL = DATE_FORMAT.parse(START).getTime
val stepL = TimeUnit.MILLISECONDS.convert(STEP, TimeUnit.MINUTES)

val loc_dict = sc.textFile(LOC_DATA).map(_.split(DICT_SEP)).map(x => (x(0), x(1))).toArray.toMap

val raw_data = sc.textFile(SIG_DATA).map(_.split(SIG_SEP).toList)
val raw_sig_map = raw_data.filter(x => x.size > 17)
val sig_map = raw_sig_map.map(x => { val loc_info = loc_dict.getOrElse(x(CGI_POS), "")
                                     if (loc_info.size > 0) {
                                       List(x(USER_ID_POS), x(START_TIME_POS),
                                           loc_info.split(" ")(0), loc_info.split(" ")(1),
                                           loc_info.split(" ")(2), loc_info.split(" ")(3),
                                           loc_info.split(" ")(4))
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

// 分窗，剔重
val split_win = time_win.map(x => List(x(0), get_frame_start(x(1))) ++ x.drop(2)).distinct

// 去掉只出现一次的用户
val all_users = split_win.groupBy(_(0))
val travel_users = all_users.filter(x => x._2.size > 1)

// 对一个用户做地理位置(城市)剔重
def merge_by_geo(travellist: List[List[String]]): List[List[String]] = {
  // 按时间排序
  val travel_sorted = travellist.sortBy(x => x(TIME_POS))
  return travel_sorted.foldLeft(List(travel_sorted(0)))( (x,y) => if ((x.last)(CITY_ID_POS) == y(CITY_ID_POS)) x else x :+ y  )
}

// 生成用户的移动路线：以一个用户为单位，包含他所有的移动路线
// RDD[(String, List[List[String]])]
// 元素示例：(460018987404199,List(List(460018987404199, 2014-12-22  06:30:00, 江门, 江门, 江门火车站, 江门火车站, R), List(460018987404199, 2014-12-22  08:45:00, 深圳, 深圳, 宝安国际机场, 宝安国际机场, A)))
val raw_travel_map = travel_users.map(x => (x._1, merge_by_geo(x._2.toList)))
val user_travel_map = raw_travel_map.filter(x => x._2.size > 1)


// 生成移动路线对，元素示例：List(460018987404199, 2014-12-22  06:30:00, 江门, 2014-12-22  08:45:00, 深圳, A)
val travel_pairs = user_travel_map.flatMap(x => (0 until x._2.size - 1).map(y => List(x._2(y)(0), x._2(y)(1), x._2(y)(2), x._2(y+1)(1), x._2(y+1)(2), x._2(y+1)(6))))

travel_pairs.saveAsTextFile(USER_MAP_FILE)
