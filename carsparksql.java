import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.List;

public class carsparkSQL {
    public static class crossMonitor 
    implements Serializable {
        private int carID;
        private int cross;
        private int timestamp;
        public int getCarID() {
            return carID;
        }
        public int getcross() {
            return cross;
        }
        public int getTimestamp() {
            return timestamp;
        }
        public void setCarID(int carID) {
            this.carID = carID;
        }
        public void setcross(int cross) {
            this.cross = cross;
        }
        public void setTimestamp(int timestamp) {
            this.timestamp = timestamp;
        }
    }


    public static void writeFileContext(List<Row>result, String path)  {
        File file = new File(path);
        
        if (!file.isFile()) {
            file.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));
        for (Row row:result){
            writer.write(row + "\r\n");
        }
        writer.close();
    }

    public static void main(String [] args)  {
        String file = args[0];



        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("spark://10.176.122.106:7077")
                .getOrCreate();

        JavaRDD<crossMonitor> carRDD = spark.read()
                .textFile("hdfs://10.176.122.106:9000/"+file)
                .javaRDD()
                .map(new Function<String, crossMonitor>() {
                    @Override
                    public crossMonitor call(String lines)  {
                        String[] line = lines.split(",");
                        int lenTimestamp = line[2].length();
                        crossMonitor crossMonitor = new crossMonitor();
                        crossMonitor.setCarID(Integer.parseInt(line[0]));
                        crossMonitor.setcross(Integer.parseInt(line[1]));
                        crossMonitor.setTimestamp(Interger.parseLong(line[2].substring(0,lenTimestamp-1)));
                        return crossMonitor;
                    }
                });

        Dataset<Row> carDF = spark.createDataFrame(carRDD, crossMonitor.class);

        carDF.create("cross");

        Dataset<Row> carIDDF1 = spark.sql(
                "SELECT car1.carID as carID1, car2.carID as carID2  " +
                        "from cross car1, cross car2 " +
                        "WHERE ABS(car1.timestamp-car2.timestamp) <=60 " +
                        "AND  car1.cross = car2.cross" +
                        "AND car1.carID < car2.carID");
        
        carIDDF1.show();
        carIDDF1.create("accompany");


        
        Dataset<Row> carIDDF2 = spark.sql(
                "SELECT carID1, carID2 " +
                        "from accompany a " +
                        "WHERE (a.carID1, a.carID2) in " +
                        "(select carID1, carID2 from accompany " +
                        "group by carID1, carID2 " +
                        "having count(*) > "+5600+" )"

                        );

        
        carIDDF2.show();
        carIDDF2.createOrReplaceTempView("pair");

        Dataset<Row> carIDDF3 = spark.sql(
                "SELECT carID1, carID2, COUNT(*) as threshold " +
                        "from pair  " +
                        "group by carID1, carID2 " +
                        "having count(*) > "+5600 +" "+
                        "ORDER BY threshold DESC"
        );

        carIDDF3.distinct();
        carIDDF3.show();
        List<Row> result =  carIDDF3.collectAsList();
        writeFileContext(result, "result.txt");

        spark.stop();
    }
}

