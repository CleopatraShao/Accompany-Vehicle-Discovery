package main.java;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

import com.google.inject.internal.util.$Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.mllib.*;
import scala.Tuple2;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.lang.String;




public class CompCar_Apriori {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Test Application")
            .set("spark.driver.memory", "20g").set("spark.driver.cores", "2").set("spark.driver.maxResultSize", "0")
            .set("spark.executor.memory", "40g").set("spark.executor.heartbeatInterval", "999999").set("spark.cleaner.ttl", "100")
            .set("spark.default.parallelism", "4").set("spark.hadoop.validateOutputSpecs", "false").set("spark.executor.cores", "2")
            .set("spark.storage.memoryFraction", "0.3").set("spark.network.timeout", "999999");

        String logFile="D:\\大三上大数据分析技术\\伴随车检测期末Project\\31.csv";
        String output_path="D:\\output.txt";
        JavaSparkContext ssc = new JavaSparkContext(conf);
        JavaRDD<List<Long>> trans = ssc.textFile(logFile).map(r -> {
            //去除格式化符号
            r = r.replace("[", "");
            r = r.replace("]", "");
            r = r.replace(" ", "");
            String[] parts = r.split(",");
            List<Long> temp = new ArrayList<>();
            //将原本的字符串转变成为long值便于后续操作
            for (String value : parts)
                temp.add(Long.parseLong(value));
            LinkedHashSet<Long> hashSet = new LinkedHashSet<>(temp);
            //形成数组队列
            ArrayList<Long> withoutDuplicates = new ArrayList<>(hashSet);
            if (!withoutDuplicates.isEmpty())
                return withoutDuplicates;
            return null;
        });

        //过滤&将非序列化RDD持久化在Storage区
        trans = trans.filter(x -> !x.isEmpty()).persist(StorageLevel.MEMORY_AND_DISK_SER());

        //Apriori算法具体过程
        //获取候选1-项集
        JavaRDD<Long> RDDa=trans.flatMap(List::iterator);
        List<List<Long>> itemset0=trans.collect();
        //生成候选1-项集
        JavaPairRDD<Long,Integer> RDDb = RDDa.mapToPair(x->new Tuple2<>(x,1));
        JavaPairRDD<Long,Integer> RDDc = RDDb.reduceByKey(Integer::sum);
        
        //手动设置最小支持度
        int minCount=5;
        //按最小支持度计数过滤，生成频繁1-项集
        JavaPairRDD<Long,Integer> RDDd=RDDc.filter(x->x._2>=minCount);

        //连接步 - 由频繁1-项集与自身连接产生候选2-项集列表
        JavaRDD<Long> RDDe=RDDd.map(x->x._1);
        List<Long> list1=RDDe.collect();
        List<List<Long>> itemset2=new ArrayList<>();
        for(int i=0;i<list1.size()-1;i++){
            for(int j=i+1;j<list1.size();j++){
                List<Long> l=new ArrayList<>();
                l.add(list1.get(i));
                l.add(list1.get(j));
                itemset2.add(l);
            }
        }
        //生成频繁2项集
        JavaRDD<List<Long>>RDDf=ssc.parallelize(itemset2);
        JavaPairRDD<List<Long>,Integer>RDD_itemset2=RDDf.mapToPair(x->new Tuple2(x,1));
        JavaPairRDD<List<Long>,Integer> RDDg = RDD_itemset2.reduceByKey(Integer::sum);
        //剪枝步 - 扫描频繁2项集，判断每一个候选的计数是否小于最小支持度计数
        JavaPairRDD<List<Long>,Integer>RDDh=RDDg.filter(x->support_count(itemset0,x._1)>=minCount);
        //将我们得到的频繁2项集写入频繁2文件
        writeFileContext(RDDh.collect(),output_path);
        JavaPairRDD<List<Long>,Integer> RDD_itemset=RDDh;



        //循环 - 由频繁k项集生成频繁k+1项集
        int k=2;
        while(RDD_itemset.count()>k){
            k=k+1;
            JavaRDD<List<Long>>RDDi=RDDg.map(x->x._1);
            List<List<Long>>list2=RDDi.collect();
            List<List<Long>>itemset=new ArrayList<>();
            //生成候选k+1项集列表
            for(int i=0;i<list2.size()-1;i++){
                for(int j=i+1;j<list2.size();j++){
                    //对于频繁k项集的任两个元素
                    //求交集，若其长度等于k+1，则加入到候选k+1项集列表
                    List<Long> l1=list2.get(i);
                    List<Long> l2=list2.get(j);
                    //求交集
                    l1.remove(l2);
                    l1.addAll(l2);
                    //判断长度
                    if(l1.size()==k)
                        itemset.add(l1);
                }
            }
            //生成频繁k+1项集
            JavaRDD<List<Long>> RDDj=ssc.parallelize(itemset);
            JavaPairRDD<List<Long>,Integer> RDDk=RDDj.mapToPair(x->new Tuple2(x,1));
            JavaPairRDD<List<Long>,Integer> RDDl = RDDk.reduceByKey(Integer::sum);
            RDD_itemset=RDDm.filter(x->support_count(itemset0,x._1)>=minCount);
            //将频繁k+1项集写入输出文件
            writeFileContext(RDD_itemset.collect(),output_path);

        }


        //测试，打印前100个频繁1-项集以及频繁1-项集大小
         for( Tuple2<String,Integer> t:RDDh.take(100))
            System.out.println(t);
        System.out.println(RDDh.count()+"\n");             

    }

    //计算支持度计数
    public static int support_count(List<List<Long>>base_set,List<Long> ls){
        int count=0;
        for(List<Long> l :base_set){
            //若基项集中的此项包含目标项，则目标项的支持度计数加1
            List<Long> temp=ls;
            temp.remove(l);
            if(temp.isEmpty())
                count++;
        }
        return count;
    }

    //将频繁项集结果写入输出文件
    //每一行为一条伴随车结果 每一行的格式为 car1 car2 ... 支持度
    public static void writeFileContext(List<Tuple2<List<Long>, Integer> > list, String path) throws Exception {
        File file = new File(path);
        //如果没有文件就创建
        if (!file.isFile()) {
            file.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));
        for(Tuple2<List<Long>, Integer> tp:list){
            for(Long s:tp._1)
                writer.write(s + " ");
            writer.write(tp._2+"\n");
        }


        writer.close();
    }
        //测试代码 试打印前100个频繁1-项集以及频繁1-项集大小
       for( Tuple2<Long,Integer> t:RDDh.take(100))
            System.out.println(t);
       System.out.println(RDDh.count()+"\n");


}

