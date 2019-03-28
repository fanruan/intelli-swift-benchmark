package com.fr.swift.benchmark.data;

import com.fr.swift.result.SwiftResultSet;
import com.fr.swift.source.ListBasedRow;
import com.fr.swift.source.Row;
import com.fr.swift.source.SwiftMetaData;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lyon on 2019-03-27.
 */
public class DataGenerators {

    public static SwiftResultSet generate(final int rowCount, final List<DataGenerator> generators, final SwiftMetaData metaData) {
        return new SwiftResultSet() {

            private int count = 0;

            @Override
            public int getFetchSize() {
                return 0;
            }

            @Override
            public SwiftMetaData getMetaData() throws SQLException {
                return metaData;
            }

            @Override
            public boolean hasNext() throws SQLException {
                return count < rowCount;
            }

            @Override
            public Row getNextRow() throws SQLException {
                List list = new ArrayList();
                for (DataGenerator generator : generators) {
                    list.add(generator.next());
                }
                count++;
                return new ListBasedRow(list);
            }

            @Override
            public void close() throws SQLException {
            }
        };
    }

    public static List<String> createBaseDim(int dimCardinality) {
        List<String> data = new ArrayList<String>();
        for (int i = 0; i < dimCardinality; i++) {
            data.add("String" + i);
        }
        return data;
    }
}
