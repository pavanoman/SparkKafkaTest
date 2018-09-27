import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.Serializable;
// $example off:spark_hive$

public class JavaSparkHive {

    // $example on:spark_hive$
    public static class Record implements Serializable {
        private int key;
        private String value;

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
    // $example off:spark_hive$

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        // $example on:spark_hive$
        // warehouseLocation points to the default location for managed databases and tables
        String warehouseLocation = new File("hive-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession

                .builder()
                .appName("Java Spark Hive")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .master("local[4]")
                .getOrCreate();

        //spark.sql("show databases").show();
        spark.sql("CREATE TABLE IF NOT EXISTS default.src (key INT, value STRING) USING hive");
        spark.sql("LOAD DATA LOCAL INPATH '/home/cloudera/Documents/MavenProjects/Test/src/main/java/testdata.txt' INTO TABLE default.src");

        // Queries are expressed in HiveQL
        spark.sql("SELECT * FROM default.src").show();
        // +---+-------+
        // |key|  value|
        // +---+-------+
        // |238|val_238|
        // | 86| val_86|
        // |311|val_311|
        // ...

        // Aggregation queries are also supported.
        spark.sql("SELECT COUNT(*) FROM default.src").show();
        // +--------+
        // |count(1)|
        // +--------+
        // |    500 |
        // +--------+


        spark.stop();
    }
}