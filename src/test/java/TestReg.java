public class TestReg {
    public static void main(String[] args) {

        String columns = "id,name,category2_id";
        System.out.println(columns.replaceAll("[^,]+", "$0 varchar"));
        System.out.println(columns.replaceAll("[^,]+","$0 varchar"));
    }
}


