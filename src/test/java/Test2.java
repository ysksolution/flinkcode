import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Test2 {
    public static void main(String[] args) {

        JSONObject obj = new JSONObject();
        obj.put("a", 97);
        obj.put("b", 98);
        obj.put("c", 99);
        obj.put("op_type", "insert");

        List<String> list = Arrays.asList("a,c".split(","));

        Set<String> keys = obj.keySet();
        /*

        //for循环只能遍历读取集合中的元素，但是不能删除，否则会有并发异常
        //java.util.ConcurrentModificationException
        for (String key : keys) {
            if (list.contains(key)) {
                keys.remove(key);
            }
        }*/

//        keys.removeIf(key -> !list.contains(key));
//        System.out.println(keys);
        /*
        //删除需要使用迭代器
        Iterator<String> it = keys.iterator();
        while (it.hasNext()) {
            String key = it.next();
            if (!list.contains(key) &&  !"op_type".equals(key)) {
                it.remove();
            }
        }
         */

        //删除需要使用迭代器
        keys.removeIf(key -> !list.contains(key) && !"op_type".equals(key));

        System.out.println(keys);


        System.out.println(keys);

    }
}
