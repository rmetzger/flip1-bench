package com.ververica.utilz;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.util.Random;

public class SlotSecondAnalyzer {
    public static void main(String[] args) {
        /**
         * Job runtime: 10 minutes (no failures)
         *
         * failures every 5 minutes
         * failures every 2.5 minutes
         * failures every 1 minute
         */
        double lambda = 1;
        ExponentialDistribution dist = new ExponentialDistribution(new Well19937c(2),1);

        SummaryStatistics stats = new SummaryStatistics();

        for (int i = 0; i < 200; i++) {
            double val = dist.sample();
            stats.addValue(val);
            System.out.println("i = " + i + " sample = " + val);
        }

        System.out.println("Summary Stats: mean = " + stats.getMean() + " sum = " + stats.getSum());

    }
}
