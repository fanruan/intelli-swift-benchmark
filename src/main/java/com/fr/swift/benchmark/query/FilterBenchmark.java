package com.fr.swift.benchmark.query;

import com.fr.swift.SwiftContext;
import com.fr.swift.base.meta.MetaDataColumnBean;
import com.fr.swift.base.meta.SwiftMetaDataBean;
import com.fr.swift.benchmark.data.DataGenerators;
import com.fr.swift.benchmark.data.NormalGenerator;
import com.fr.swift.benchmark.data.RandomGenerator;
import com.fr.swift.benchmark.data.SequentialGenerator;
import com.fr.swift.benchmark.env.EnvUtils;
import com.fr.swift.config.service.SwiftMetaDataService;
import com.fr.swift.db.Table;
import com.fr.swift.query.QueryRunnerProvider;
import com.fr.swift.query.filter.SwiftDetailFilterType;
import com.fr.swift.query.info.bean.element.DimensionBean;
import com.fr.swift.query.info.bean.element.filter.impl.AndFilterBean;
import com.fr.swift.query.info.bean.element.filter.impl.InFilterBean;
import com.fr.swift.query.info.bean.element.filter.impl.NumberInRangeFilterBean;
import com.fr.swift.query.info.bean.element.filter.impl.OrFilterBean;
import com.fr.swift.query.info.bean.element.filter.impl.StringOneValueFilterBean;
import com.fr.swift.query.info.bean.query.DetailQueryInfoBean;
import com.fr.swift.query.info.bean.type.DimensionType;
import com.fr.swift.result.SwiftResultSet;
import com.fr.swift.segment.insert.HistoryBlockImporter;
import com.fr.swift.source.SourceKey;
import com.fr.swift.source.SwiftMetaData;
import com.fr.swift.source.SwiftMetaDataColumn;
import com.fr.swift.source.alloter.impl.line.HistoryLineSourceAlloter;
import com.fr.swift.source.alloter.impl.line.LineAllotRule;
import com.fr.swift.source.resultset.progress.ProgressResultSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by lyon on 2019-03-28.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms512M", "-Xmx2G"})
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class FilterBenchmark {
    @Param({"1000000"})
    private int rowCount;

    @Param({"100"})
    private int dimCardinality = 100;

    private List<String> dimValues;
    private static String tableName = "test";
    private static Random random = new Random(37);

    public static void main(String[] args) throws Exception {

        Options opt = new OptionsBuilder()
                .include(FilterBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        // start engine
        EnvUtils.start();

        // prepare metadata
        dimValues = DataGenerators.createBaseDim(dimCardinality);
        SwiftMetaDataService service = SwiftContext.get().getBean(SwiftMetaDataService.class);
        SwiftMetaData metaData = new SwiftMetaDataBean(tableName, Arrays.<SwiftMetaDataColumn>asList(
                new MetaDataColumnBean("a", Types.VARCHAR),
                new MetaDataColumnBean("b", Types.INTEGER),
                new MetaDataColumnBean("c", Types.DOUBLE)
        ));
        service.addMetaData(tableName, metaData);
        SwiftResultSet resultSet = DataGenerators.generate(rowCount, Arrays.asList(
                new SequentialGenerator(dimValues),
                new RandomGenerator(dimCardinality),
                new NormalGenerator(10000, 100)
        ), service.getMetaDataByKey(tableName));

        // import data
        HistoryLineSourceAlloter alloter = new HistoryLineSourceAlloter(new SourceKey(tableName), new LineAllotRule());
        Table t = com.fr.swift.db.impl.SwiftDatabase.getInstance().getTable(new SourceKey(tableName));
        HistoryBlockImporter importer = new HistoryBlockImporter(t, alloter);
        importer.importData(new ProgressResultSet(resultSet, tableName));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        EnvUtils.clearTable(tableName);
        EnvUtils.stop();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void range(Blackhole bh) throws Exception {
        DetailQueryInfoBean query = DetailQueryInfoBean.builder(tableName)
                .setDimensions(new DimensionBean(DimensionType.DETAIL_ALL_COLUMN))
                .setFilter(
                        NumberInRangeFilterBean.builder("b")
                                .setStart(Integer.toString(random.nextInt(dimCardinality)), true)
                                .setEnd(Integer.toString(random.nextInt(dimCardinality)), false)
                                .build()
                )
                .build();
        SwiftResultSet resultSet = QueryRunnerProvider.getInstance().query(query);
        while (resultSet.hasNext()) {
            bh.consume(resultSet.getNextRow());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void In(Blackhole bh) throws Exception {
        DetailQueryInfoBean query = DetailQueryInfoBean.builder(tableName)
                .setDimensions(new DimensionBean(DimensionType.DETAIL_ALL_COLUMN))
                .setFilter(new InFilterBean("a",
                        dimValues.get(random.nextInt(dimValues.size())),
                        dimValues.get(random.nextInt(dimValues.size())),
                        dimValues.get(random.nextInt(dimValues.size())),
                        dimValues.get(random.nextInt(dimValues.size())),
                        dimValues.get(random.nextInt(dimValues.size()))
                ))
                .build();
        SwiftResultSet resultSet = QueryRunnerProvider.getInstance().query(query);
        while (resultSet.hasNext()) {
            bh.consume(resultSet.getNextRow());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void Like(Blackhole bh) throws Exception {
        DetailQueryInfoBean query = DetailQueryInfoBean.builder(tableName)
                .setDimensions(new DimensionBean(DimensionType.DETAIL_ALL_COLUMN))
                .setFilter(new StringOneValueFilterBean("a",
                        SwiftDetailFilterType.STRING_LIKE,
                        Integer.toString(random.nextInt(dimValues.size()))
                ))
                .build();
        SwiftResultSet resultSet = QueryRunnerProvider.getInstance().query(query);
        while (resultSet.hasNext()) {
            bh.consume(resultSet.getNextRow());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void startWith(Blackhole bh) throws Exception {
        DetailQueryInfoBean query = DetailQueryInfoBean.builder(tableName)
                .setDimensions(new DimensionBean(DimensionType.DETAIL_ALL_COLUMN))
                .setFilter(new StringOneValueFilterBean("a",
                        SwiftDetailFilterType.STRING_STARTS_WITH,
                        dimValues.get(random.nextInt(dimValues.size()))
                ))
                .build();
        SwiftResultSet resultSet = QueryRunnerProvider.getInstance().query(query);
        while (resultSet.hasNext()) {
            bh.consume(resultSet.getNextRow());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void endWith(Blackhole bh) throws Exception {
        DetailQueryInfoBean query = DetailQueryInfoBean.builder(tableName)
                .setDimensions(new DimensionBean(DimensionType.DETAIL_ALL_COLUMN))
                .setFilter(new StringOneValueFilterBean("a",
                        SwiftDetailFilterType.STRING_ENDS_WITH,
                        Integer.toString(random.nextInt(dimValues.size()))
                ))
                .build();
        SwiftResultSet resultSet = QueryRunnerProvider.getInstance().query(query);
        while (resultSet.hasNext()) {
            bh.consume(resultSet.getNextRow());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void and(Blackhole bh) throws Exception {
        DetailQueryInfoBean query = DetailQueryInfoBean.builder(tableName)
                .setDimensions(new DimensionBean(DimensionType.DETAIL_ALL_COLUMN))
                .setFilter(
                        new AndFilterBean(
                                Arrays.asList(
                                        NumberInRangeFilterBean.builder("b")
                                                .setStart(Integer.toString(random.nextInt(dimCardinality)), true)
                                                .setEnd(Integer.toString(random.nextInt(dimCardinality)), false)
                                                .build(),
                                        new InFilterBean("a",
                                                dimValues.get(random.nextInt(dimValues.size())),
                                                dimValues.get(random.nextInt(dimValues.size())),
                                                dimValues.get(random.nextInt(dimValues.size())),
                                                dimValues.get(random.nextInt(dimValues.size())),
                                                dimValues.get(random.nextInt(dimValues.size()))
                                        )
                                )
                        )
                )
                .build();
        SwiftResultSet resultSet = QueryRunnerProvider.getInstance().query(query);
        while (resultSet.hasNext()) {
            bh.consume(resultSet.getNextRow());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void or(Blackhole bh) throws Exception {
        DetailQueryInfoBean query = DetailQueryInfoBean.builder(tableName)
                .setDimensions(new DimensionBean(DimensionType.DETAIL_ALL_COLUMN))
                .setFilter(
                        new OrFilterBean(
                                Arrays.asList(
                                        NumberInRangeFilterBean.builder("b")
                                                .setStart(Integer.toString(random.nextInt(dimCardinality)), true)
                                                .setEnd(Integer.toString(random.nextInt(dimCardinality)), false)
                                                .build(),
                                        new InFilterBean("a",
                                                dimValues.get(random.nextInt(dimValues.size())),
                                                dimValues.get(random.nextInt(dimValues.size())),
                                                dimValues.get(random.nextInt(dimValues.size())),
                                                dimValues.get(random.nextInt(dimValues.size())),
                                                dimValues.get(random.nextInt(dimValues.size()))
                                        )
                                )
                        )
                )
                .build();
        SwiftResultSet resultSet = QueryRunnerProvider.getInstance().query(query);
        while (resultSet.hasNext()) {
            bh.consume(resultSet.getNextRow());
        }
    }
}
