package com.fr.swift.benchmark.data;

import java.util.List;
import java.util.Random;

/**
 * Created by lyon on 2019-03-27.
 */
public class RandomGenerator implements DataGenerator<Object> {

    private Random random = new Random(37);
    private int bound;
    private List<Object> values;

    public RandomGenerator(int bound) {
        this.bound = bound;
    }

    public RandomGenerator(List values) {
        this.values = values;
    }

    @Override
    public Object next() {
        if (values == null) {
            return (long) random.nextInt(bound);
        }
        return values.get(random.nextInt(values.size()));
    }
}
