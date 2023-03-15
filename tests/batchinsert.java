package tests;

public class batchinsert {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage - java batchinsert DATAFILENAME TYPE BIGTABLENAME");
            return;
        }
        String dataFileName = args[0];
        int type = Integer.parseInt(args[1]);
        String bigTableName = args[2];
    }
}
