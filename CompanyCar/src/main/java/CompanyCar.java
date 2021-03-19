package main.java;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.storage.StorageLevel;


import java.util.ArrayList;
import java.util.List;
import java.lang.String;


public class CompanyCar {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Test Application");
//                .set("spark.driver.memory", "5g").set("spark.driver.cores", "4").set("spark.driver.maxResultSize", "0")
//                .set("spark.executor.memory", "20g").set("spark.cleaner.ttl", "100")
//            //    .set("spark.default.parallelism", "4").set("spark.hadoop.validateOutputSpecs", "false").set("spark.executor.cores", "2")
//                .set("spark.storage.memoryFraction", "0.6").set("spark.executor.cores","8")
//                .set("spark.shuffle.manager","tungsten-sort").set("spark.storage.storageFraction","0.3")
//                .set("spark.memory.offHeap.enabled","true").set("spark.memory.offHeap.size","1g");
//        // .set("spark.executor.heartbeatInterval", "9999999") .set("spark.network.timeout", "9999999")

//读取结构化数据
        String logFile=args[0];
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<List<Long>> transactions = sc.textFile(logFile).map(s -> {
            //去除格式化符号
            s = s.replace("[", "");
            s = s.replace("]", "");
            s = s.replace(" ", "");
            String[] parts = s.split(",");
            List<Long> tmp = new ArrayList<>();
            for (String value : parts)
                    tmp.add(Long.parseLong(value));
            LinkedHashSet<Long> hashSet = new LinkedHashSet<>(tmp);
            ArrayList<Long> l_withoutDuplicates = new ArrayList<>(hashSet);
            if (!l_withoutDuplicates.isEmpty())
                return l_withoutDuplicates;
            return null;
        });
        transactions = transactions.filter(x -> !x.isEmpty());
       // transactions = transactions.filter(x -> !x.isEmpty()).persist(StorageLevel.MEMORY_AND_DISK_SER());

        //spark结构化数据生成部分,在执行完之后注释掉
//        JavaPairRDD<Long,Tuple2<Long,Long>> RDD1 = sc.textFile(logFile)
//                .mapToPair((PairFunction<String, Long, Tuple2<Long, Long>>) s -> {
//                    s=s.replace(" ","");
//                    String[] parts = s.split(",");
//                     //以路口id为关键词，生成(cross_id,(timestamp,car_id))  
//                    Tuple2<Long,Long> tp1=new Tuple2<Long, Long>(Long.parseLong(parts[2]),Long.parseLong(parts[0]));
//                    return new Tuple2<>(Long.parseLong(parts[1]), tp1);
//                });
//
//        //RDD1=RDD1.distinct(); //去重
//        JavaPairRDD<Long,Iterable<Tuple2<Long,Long>>>RDD2=RDD1.groupByKey(); //按照路口id分组
//        //  现在得到了<cross_id,iterable<(timestamp,car_id)>格式的数据
//        //  按timestamp升序排序
//        JavaPairRDD<Long, Iterable<Tuple2<Long, Long>>> sorted_RDD = RDD2.mapValues(v1-> {
//                List<Tuple2<Long, Long>> newList = Lists.newArrayList(v1);
//                newList.sort(new Tuplecompatrtor());
//                return newList;
//            });
//
//         //使用map，对每个路口从起始时间开始移动时间窗口，得到每个路口的窗口伴随集合
//        JavaRDD<List<List<Long>>>RDD3=sorted_RDD
//                .map(t -> {
//                    List<List<Long>> l_list = new ArrayList<>();
//                    List<Tuple2<Long, Long>> newList=Lists.newArrayList(t._2);
//                    long startTime=newList.get(0)._1;
//                    for(int i=0;i<newList.size();){
//                        List<Long> l= new ArrayList<>();
//
//                        int j=i;
//                        for(;j<newList.size() && newList.get(j)._1-startTime<60;++j){
//                            l.add(newList.get(j)._2);
//                        }
//                        if(l.size()>1) {
//                            LinkedHashSet<Long> hashSet = new LinkedHashSet<>(l);
//                            ArrayList<Long> l_withoutDuplicates = new ArrayList<>(hashSet);
//                            l_list.add(l_withoutDuplicates);
//                        }
//                        i=j;
//                        startTime+=60;
//                    }
//                    return l_list;
//                });
//
//        //去除空项
//        JavaRDD<List<List<Long>>> RDD4=RDD3.filter(x->!x.isEmpty());
//
//        //展开，生成原始数据集
//        JavaRDD<List<Long>>transactions=RDD3.flatMap(List::iterator);
//        System.out.println("Data constructed terminating! Now write it to file.");
//        transactions.saveAsTextFile("/structData");
//        System.out.println("Everything is OK, now terminate the process");
//        transactions=transactions.distinct();

        //FPGrowth-Tree算法
        System.out.println("Read Data terminated! Now count that.");
        long num = transactions.count();
        //设置最小置信度
        double minConfidence=new Double(args[1]);
        System.out.println("Count Data terminated! Now execute FP-growth.");
        System.out.println("Count Number is " + num);
        //设置最小支持度
        double minSupport = new Double(args[2]);
        int numPartition = 10;
        FPGrowth fpGrowth = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartition);
        FPGrowthModel<Long> model = fpGrowth.run(transactions);
        JavaRDD<AssociationRules.Rule<Long>> ruledata = model.generateAssociationRules(minConfidence).toJavaRDD();
        ruledata.saveAsTextFile("hdfs://10.176.122.106:9000/fp_res");
        //ruledata.foreach(x->System.out.println(x.toString()));
    }
}
