package chapter05_watermark.pojo;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Smexy on 2022/10/24
 */
public class MyUtil
{
    public static <T> List<T> toList(Iterable<T> elements){
        List<T> result =  new ArrayList<>();
        for (T element : elements) {
            result.add(element);
        }
        return result;
    }

    public static String printTimeWindow(TimeWindow t){

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        return "窗口[" + format.format(new Date(t.getStart())) + "," + format.format(new Date(t.getEnd())) + ")";

    }
}
