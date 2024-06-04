import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;

public class Cameral {
    public static void main(String[] args) {
        String name = "keywordCount";
         name = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);
        System.out.println(name);
    }

}
