package com.fr.swift.benchmark.data;

import java.util.List;

/**
 * Created by lyon on 2019-03-27.
 */
public class SequentialGenerator<T> implements DataGenerator<T> {

    private int count = 0;
    private List<Object> values;

    public SequentialGenerator(List<Object> values) {
        this.values = values;
    }

    @Override
    public T next() {
        T ret = (T) values.get(count++);
        if (count >= values.size()) {
            count = 0;
        }
        return ret;
    }
}
