package com.fr.swift.benchmark.env;

import com.fr.swift.SwiftContext;
import com.fr.swift.boot.SwiftEngineStart;
import com.fr.swift.config.SwiftConfig;
import com.fr.swift.config.SwiftConfigConstants;
import com.fr.swift.config.entity.SwiftConfigEntity;
import com.fr.swift.config.query.SwiftConfigEntityQueryBus;
import com.fr.swift.config.service.SwiftMetaDataService;
import com.fr.swift.config.service.SwiftSegmentService;
import com.fr.swift.context.ContextProvider;
import com.fr.swift.property.SwiftProperty;
import com.fr.swift.source.SourceKey;
import com.fr.swift.util.concurrent.SwiftExecutors;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by lyon on 2019-03-28.
 */
// TODO: 2019-03-28 内置保存metadata的数据库
public class EnvUtils {

    private static final String CUBE = "cubes";

    public static void start() {
        SwiftProperty property = SwiftProperty.getProperty();
        property.setServerServiceNames(new HashSet<String>(Arrays.asList("analyse", "indexing", "history")));
        SwiftEngineStart.start(new String[0]);
    }

    public static void stop() {
        // 清理cubes文件
        final String contextPath = SwiftContext.get().getBean(ContextProvider.class).getContextPath();
        final SwiftConfigEntityQueryBus query = (SwiftConfigEntityQueryBus) SwiftContext.get().getBean(SwiftConfig.class).query(SwiftConfigEntity.class);
        File file = new File(query.select(SwiftConfigConstants.Namespace.SWIFT_CUBE_PATH, String.class, contextPath) + File.separator + CUBE);
        try {
            FileUtils.deleteDirectory(file);
        } catch (Exception ignored) {
        }

        // 停止swift内部线程
        SwiftExecutors.shutdownAllNow();

        // TODO: 2019-03-28 FineIO的线程不能正常退出，只能等超时
    }

    public static void clearTable(String... tables) {
        for (String table : tables) {
            SwiftSegmentService segmentService = SwiftContext.get().getBean("segmentServiceProvider", SwiftSegmentService.class);
            segmentService.removeSegments(table);
            SwiftMetaDataService service = SwiftContext.get().getBean(SwiftMetaDataService.class);
            service.removeMetaDatas(new SourceKey(table));
        }
    }
}
