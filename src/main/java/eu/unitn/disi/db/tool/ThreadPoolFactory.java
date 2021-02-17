package eu.unitn.disi.db.tool;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

public class ThreadPoolFactory {
    private static ExecutorService WILDCARD_SEARCH_THREAD_POOL = null;
    private static ExecutorService WILDCARD_THREAD_POOL = null;
    private static ExecutorService TABLE_COMPUTE_THREAD_POOL = null;
    private static ExecutorService SEARCH_THREAD_POOL = null;
    private static ForkJoinPool FORK_JOIN_POOL = null;

    public static ExecutorService getWildcardThreadPool() {
        if (WILDCARD_THREAD_POOL == null) {
            WILDCARD_THREAD_POOL = Executors.newFixedThreadPool(8);
        }
        return WILDCARD_THREAD_POOL;
    }

    public synchronized static ForkJoinPool getForkJoinPool() {
        if (FORK_JOIN_POOL == null) {
            FORK_JOIN_POOL = new ForkJoinPool(84);
        }
        return FORK_JOIN_POOL;
    }

    public synchronized static ExecutorService getWildcardSearchThreadPool() {
        if (WILDCARD_SEARCH_THREAD_POOL == null) {
            WILDCARD_SEARCH_THREAD_POOL = Executors.newFixedThreadPool(8);
        }
        return WILDCARD_SEARCH_THREAD_POOL;
    }


    public synchronized static ExecutorService getTableComputeThreadPool() {
        if (TABLE_COMPUTE_THREAD_POOL == null) {
            TABLE_COMPUTE_THREAD_POOL = Executors.newFixedThreadPool(8);
        }
        return TABLE_COMPUTE_THREAD_POOL;
    }

    public synchronized static ExecutorService getSearchThreadPool() {
        if (SEARCH_THREAD_POOL == null) {
            SEARCH_THREAD_POOL = Executors.newFixedThreadPool(1);
        }
        return SEARCH_THREAD_POOL;
    }

    public static void shutdownAll() {
        Optional.ofNullable(WILDCARD_THREAD_POOL).ifPresent(ExecutorService::shutdown);
        Optional.ofNullable(TABLE_COMPUTE_THREAD_POOL).ifPresent(ExecutorService::shutdown);
        Optional.ofNullable(SEARCH_THREAD_POOL).ifPresent(ExecutorService::shutdown);
        Optional.ofNullable(WILDCARD_SEARCH_THREAD_POOL).ifPresent(ExecutorService::shutdown);
        Optional.ofNullable(FORK_JOIN_POOL).ifPresent(ExecutorService::shutdown);
    }
}
