package main.java;

import scala.Tuple2;

import java.util.Comparator;

public class Tuplecompatrtor implements Comparator<Tuple2<Long, Long>> {
    public int compare(Tuple2<Long, Long> o1, Tuple2<Long, Long> o2) {
        return new Long(o1._1-o2._1).intValue();
    }
}