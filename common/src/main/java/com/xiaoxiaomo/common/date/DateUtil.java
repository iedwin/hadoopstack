package com.xiaoxiaomo.common.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 提供通用的日期获取方法
 * Created by xiaoxiaomo on 17/4/29.
 */
public class DateUtil {
    public static String getNowTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    public static String getNowDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date());
    }

    public static Date parseString2Date(String d) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.parse(d);
    }

    public static Date parseString2Date2(String d) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.parse(d);
    }

    public static String parseDate2String(Date d) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(d);
    }

    public static Long getTimestamp(String d) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.parse(d).getTime();
    }


    public static String parseDate2String2(Date d) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(d);
    }

    /**
     * @param year
     * @param week
     * @return
     */
    public static Date getWeekLastDayOfYear(int year, int week) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.WEEK_OF_YEAR, week);
        int min = calendar.getActualMinimum(Calendar.DAY_OF_WEEK) + 1; // 获取周开始基准
        int current = calendar.get(Calendar.DAY_OF_WEEK); // 获取当天周内天数
        calendar.add(Calendar.DAY_OF_WEEK, min - current); // 当天-基准，获取周开始日期
        Date start = calendar.getTime();
        calendar.add(Calendar.DAY_OF_WEEK, 6); // 开始+6，获取周结束日期
        Date end = calendar.getTime();
        return end;
    }

    public static Date getMonthLastDayOfYear(int year, int month) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.add(Calendar.DAY_OF_MONTH, -1);
        Date lastDate = cal.getTime();
        return lastDate;
    }

    public static List<String> getLoopDate(String timeA, String timeB) {
        List<String> datelist = new ArrayList<String>();
        SimpleDateFormat formater = new SimpleDateFormat("yyyyMMdd");

        Date dateBegin = null;
        Date dateEnd = null;
        try {
            dateBegin = formater.parse(timeA);
            dateEnd = formater.parse(timeB);
        } catch (ParseException e1) {
            e1.printStackTrace();
        }
        int c = 0;
        Calendar ca = Calendar.getInstance();
        while (dateBegin.compareTo(dateEnd) < 0) {
            ca.setTime(dateBegin);
            if (c == 0) {
                ca.add(Calendar.DATE, 0);// 把dateBegin加上1天然后重新赋值给date1
            } else {
                ca.add(Calendar.DATE, 1);
            }
            dateBegin = ca.getTime();
            datelist.add(formater.format(dateBegin));
            c++;
        }
        return datelist;
    }

    public static String getTomorrow(String day, String format) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date;
        try {
            date = simpleDateFormat.parse(day);
        } catch (ParseException e) {
            e.printStackTrace();
            return "format mismatching";
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(date.getTime());
        calendar.add(calendar.DATE, 1);
        date = calendar.getTime();
        return simpleDateFormat.format(date);
    }

    public static String getDaysBefore(String day, String format) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date;
        try {
            date = simpleDateFormat.parse(day);
        } catch (ParseException e) {
            e.printStackTrace();
            return "format mismatching";
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(date.getTime());
        calendar.add(calendar.DATE, -1);
        date = calendar.getTime();
        return simpleDateFormat.format(date);
    }

    public static void main(String[] args) {
            System.out.println(getTomorrow("2016-2-28","yyyy-MM-dd"));
            System.out.println(getDaysBefore("2016-2-28","yyyy-MM-dd"));
    }
}
