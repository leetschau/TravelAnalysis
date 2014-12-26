import java.util.Date
import java.util.Calendar
import java.util.concurrent.TimeUnit

val START = "2014-05-03 00:00:00"
val END = "2014-12-01 00:00:00"
val STEP = 15 // unit: minute
val DATE_FORMAT = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
val CITY_ID_POS = 2
val NODE_ID_POS = 4

val startCal = Calendar.getInstance()
val endCal = Calendar.getInstance() 
startCal.setTime(DATE_FORMAT.parse(START))
endCal.setTime(DATE_FORMAT.parse(END))
val startL = DATE_FORMAT.parse(START).getTime
val stepL = TimeUnit.MILLISECONDS.convert(STEP, TimeUnit.MINUTES)

def get_frame_start(time_pnt: String): String = {
  val curL = DATE_FORMAT.parse(time_pnt).getTime
  val offset = (curL - startL) % stepL
  val frame_start = new Date()
  frame_start.setTime(curL - offset)
  return DATE_FORMAT.format(frame_start.getTime)
}

val data = sc.textFile("travel.csv").map(_.split(",").toList)
val users = data.map(_(0)).distinct.toArray

def valid_time(inp_time: String): Boolean = {
    val inp_cal = Calendar.getInstance()
    inp_cal.setTime(DATE_FORMAT.parse(inp_time))
    if(inp_cal.after(startCal) && inp_cal.before(endCal)) true else false
}

def build_trv_rec(userid: String): List[String] = {
  // 针对某一用户的时间窗口内剔重
  val user_rec = data.map{ case `userid` :: x if valid_time(x(0)) => get_frame_start(x(0)) :: x.drop(1)
                        case _ => Nil
                      }.filter(_ != Nil).distinct.sortBy(x => x(0))

  // 针对某一用户的地理位置剔重
  val user_places = user_rec.groupBy(_(NODE_ID_POS)).map(x => x._2.toList(0)).sortBy(_(0)).toArray.toList

  // 生成用户旅行起始位置对
  val user_trv_pairs = (0 until user_places.size - 1).map(x => (user_places(x), user_places(x+1)))
  // 剔除城市内旅行
  val user_trv_btw_cities = user_trv_pairs.filter(x => x._1(CITY_ID_POS) != x._2(CITY_ID_POS))
  // 生成旅行标记
  val user_trv_id = user_trv_btw_cities.map(x => x._1(CITY_ID_POS) + "-" + x._2(CITY_ID_POS)).toList
  return user_trv_id
}

// 统计起始对数量
val total_trv_pairs = users.flatMap(x => build_trv_rec(x))
val result = total_trv_pairs.groupBy(l => l).map(t => (t._1, t._2.length))


