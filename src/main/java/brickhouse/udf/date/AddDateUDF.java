package brickhouse.udf.date;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;


@Description(name = "add_date",
        value = "_FUNC_(start_date, num_days,interval) - Returns the date that is num intervals after start_date.",
        extended = "for hour ,return yyyyMMddHH" +
                "   for minute ,return yyyyMMddHHmm")
public class AddDateUDF extends UDF {

    private static final Logger LOG = Logger.getLogger(AddDateUDF.class);
    private static final DateTimeFormatter YYYYMMDD = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMdd");

    public String evaluate(String dateStr, int num,String interval) throws ParseException {
        SimpleDateFormat df =null;
        SimpleDateFormat raw = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        raw.setTimeZone(TimeZone.getTimeZone("GMT+8"));

        Long time=raw.parse(dateStr).getTime();
        switch (interval){
            case "m":
            case "minute":
                time+=num*60*1000;
                df= new SimpleDateFormat("yyyyMMddHHmm");
                break;
            case "h":
            case "hour":
                time+=num*3600*1000;
                df= new SimpleDateFormat("yyyyMMddHH");
               break;
            default:
                break;
        }
        df.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        return df.format(new Date(time));
    }
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat df =null;
        SimpleDateFormat raw = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        raw.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        Long time=raw.parse("2011-12-22 22:22:11").getTime();
        System.out.println(time);
    }

}