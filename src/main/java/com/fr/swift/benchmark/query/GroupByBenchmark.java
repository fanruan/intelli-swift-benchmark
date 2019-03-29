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
import com.fr.swift.query.aggregator.AggregatorType;
import com.fr.swift.query.info.bean.element.DimensionBean;
import com.fr.swift.query.info.bean.element.MetricBean;
import com.fr.swift.query.info.bean.query.GroupQueryInfoBean;
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
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms512M", "-Xmx2G"})
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class GroupByBenchmark {

    @Param({"1000000"})
    private int rowCount;

    @Param({"200"})
    private int dimCardinality;

    private List<String> dimValues;
    private static String tableName = "test";

    public static void main(String[] args) throws Exception {

        Options opt = new OptionsBuilder()
                .include(GroupByBenchmark.class.getSimpleName())
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
                new MetaDataColumnBean("b", Types.VARCHAR),
                new MetaDataColumnBean("c", Types.DOUBLE)
        ));
        service.addMetaData(tableName, metaData);
        SwiftResultSet resultSet = DataGenerators.generate(rowCount, Arrays.asList(
                new SequentialGenerator(dimValues),
                new RandomGenerator(dimValues),
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
    public void groupByOneDim(Blackhole bh) throws Exception {
        GroupQueryInfoBean query = GroupQueryInfoBean.builder(tableName)
                .setDimensions(new DimensionBean(DimensionType.GROUP, "a"))
                .setAggregations(new MetricBean("c", AggregatorType.SUM))
                .build();
        SwiftResultSet resultSet = QueryRunnerProvider.getInstance().query(query);
        while (resultSet.hasNext()) {
            bh.consume(resultSet.getNextRow());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void groupByTwoDim(Blackhole bh) throws Exception {
        GroupQueryInfoBean query = GroupQueryInfoBean.builder(tableName)
                .setDimensions(
                        new DimensionBean(DimensionType.GROUP, "a"),
                        new DimensionBean(DimensionType.GROUP, "b")
                )
                .setAggregations(new MetricBean("c", AggregatorType.SUM))
                .build();
        SwiftResultSet resultSet = QueryRunnerProvider.getInstance().query(query);
        while (resultSet.hasNext()) {
            bh.consume(resultSet.getNextRow());
        }
    }

}
