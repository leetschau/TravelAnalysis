import java.util.Date
import java.util.Calendar
import java.util.concurrent.TimeUnit

val SIG_DATA = "travelTest.csv"
val LOC_DATA = "loc_map.csv"

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

val startCal = Calendar.getInstance()
val endCal = Calendar.getInstance() 
startCal.setTime(DATE_FORMAT.parse(START))
endCal.setTime(DATE_FORMAT.parse(END))
val startL = DATE_FORMAT.parse(START).getTime
val stepL = TimeUnit.MILLISECONDS.convert(STEP, TimeUnit.MINUTES)

val loc_dict = sc.textFile(LOC_DATA).map(_.split(" ,")).map(x => (x(0), x(1))).toArray.toMap

val raw_data = sc.textFile(SIG_DATA).map(_.split("\"").toList)
val raw_sig_map = raw_data.filter(x => x.size > 17)
val sig_map = raw_sig_map.map(x => { val loc_info = loc_dict(x(CGI_POS))
                                     Array(x(USER_ID_POS), x(START_TIME_POS),
                                           loc_info.split(" ")(0), loc_info.split(" ")(1),
                                           loc_info.split(" ")(2), loc_info.split(" ")(3))
                                   }).map(x => x.toList)

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
// val time_sorted = split_win.sortBy(x => x(1))

// 去掉只出现一次的用户
val all_users = split_win.groupBy(_(0))
val travel_users = all_users.filter(x => x._2.size > 1)

// 对一个用户做地理位置剔重
def merge_by_geo(travellist: List[List[String]]): List[String] = {
  // 按时间排序
  val travel_sorted = travellist.sortBy(x => x(TIME_POS))
  val city_list = travel_sorted.map(x => x(CITY_ID_POS))
  return city_list.foldLeft(List(""))((x,y) => if (x.last == y) x else x :+ y).drop(1)
}

// 生成用户的移动路线：(user1: List(city1, city2, ...)), (user2, ...), ...
val raw_travel_map = travel_users.map(x => (x._1, merge_by_geo(x._2.toList)))
val user_travel_map = raw_travel_map.filter(x => x._2.size > 1)
