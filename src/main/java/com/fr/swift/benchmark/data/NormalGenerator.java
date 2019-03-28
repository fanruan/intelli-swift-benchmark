package com.fr.swift.benchmark.data;

import org.apache.commons.math3.distribution.NormalDistribution;

/**
 * Created by lyon on 2019-03-27.
 */
public class NormalGenerator implements DataGenerator<Double> {

    private NormalDistribution distribution;

    public NormalGenerator(double mean, double sd) {
        this.distribution = new NormalDistribution(mean, sd);
    }

    @Override
    public Double next() {
        return distribution.sample();
    }
}
