package eu.unitn.disi.db.grava.scc;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.StartingNodeAlgorithm;
import eu.unitn.disi.db.exemplar.core.StartingNodeBaseAlgorithm;
import eu.unitn.disi.db.exemplar.core.StartingNodePathFreqAlgorithm;
import eu.unitn.disi.db.exemplar.core.algorithms.CompletableIsomorphicQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.IsomorphicQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.PruningAlgorithm;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.query.WildCardQuery;
import eu.unitn.disi.db.tool.ThreadPoolFactory;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class BFWildCardAlgorithm {

    private static final int AVG_DEGREE = 9;
    private static final int MAX_DEGREE = 688;
    private final int repititions;
    private final int threshold;
    private final int threadsNum;
    private final int neighbourNum;
    private final String queryName;
    private final Multigraph G;
    private ComputeGraphNeighbors graphTableAlgorithm;

    public BFWildCardAlgorithm(int repititions, int threshold, int threadsNum,
                               int neighbourNum, Multigraph G, String queryName) {
        this.repititions = repititions;
        this.threshold = threshold;
        this.threadsNum = threadsNum;
        this.neighbourNum = neighbourNum;
        this.queryName = queryName;
        this.G = G;
    }

    public void runWildCard(ExecutorService executorService) throws IOException, ParseException,
            AlgorithmExecutionException {

        StopWatch total = new StopWatch();
        total.start();
        Set<RelatedQuery> relatedQueries = new HashSet<>(1000);
        System.out.println("starting point " + total.getElapsedTimeMillis());
        if (threshold != 0) {
            WildCardQuery wcq = new WildCardQuery(threshold);
            wcq.run(queryName);
            Set<Multigraph> wildCardQueries = new LinkedHashSet<>(wcq.getWcQueries());
            System.out.println("after wc point " + total.getElapsedTimeMillis());
            Iterator<Multigraph> iterator = wildCardQueries.iterator();
            int size = wildCardQueries.size();
            List<CompletableFuture<Set<RelatedQuery>>> tasks = new ArrayList<>();
            for (int i = 0; i < size; i++) {

                Multigraph currentQuery = iterator.next();
                CompletableFuture<List<RelatedQuery>> task = new CompletableFuture<>();
                Callable<Set<RelatedQuery>> work = createWork(G, currentQuery, total);
                tasks.add(CompletableFuture.supplyAsync(() -> {try {
                    return work.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }}, executorService));


                System.out.println(Thread.currentThread() + ":one task " + total.getElapsedTimeMillis());

            }
            System.out.println(Thread.currentThread() + ":created tasks point " + total.getElapsedTimeMillis());
            try {
                CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0])).join();
                System.out.println("join all tasks point " + total.getElapsedTimeMillis());
                for (CompletableFuture<Set<RelatedQuery>> task : tasks) {
                    long start = Instant.now().toEpochMilli();
                    relatedQueries.addAll(task.get());
                    System.out.println("join one batch " + (Instant.now().toEpochMilli() - start));
                }

            } catch (InterruptedException e) {
                throw new RuntimeException("ie ", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("ee", e);
            }
        } else {

        }

        System.out.println(queryName + " total takes " + total.getElapsedTimeMillis()
                + " answer size:" + relatedQueries.size());
    }

    private Callable<Set<RelatedQuery>> createWork(Multigraph graph, Multigraph wildCardQuery, StopWatch watch)
            throws AlgorithmExecutionException {
        return () -> {
            Long startingNode;
            IsomorphicQuerySearch edAlgorithm = new IsomorphicQuerySearch();
            for (int exprimentTime1 = 0; exprimentTime1 < repititions; exprimentTime1++) {

                StartingNodeAlgorithm startingNodeAlgorithm = new StartingNodeBaseAlgorithm(G.getLabelFreq());

                startingNode = startingNodeAlgorithm.getStartingNodes(wildCardQuery).get(0);
//						InfoNode info = new InfoNode(startingNode);
                // System.out.println("starting node:" + startingNode);
                PruningAlgorithm pruningAlgorithm = new PruningAlgorithm();
                // Set starting node according to sels of nodes.
                pruningAlgorithm.setStartingNode(startingNode);
                pruningAlgorithm.setGraph(graph);
                pruningAlgorithm.setQuery(wildCardQuery);
                pruningAlgorithm.setK(neighbourNum);
                pruningAlgorithm.setgPathTables(graphTableAlgorithm.getPathTables());

                pruningAlgorithm.setThreshold(0);
                pruningAlgorithm.computeWithPath(watch);
                System.out.println(queryName + Thread.currentThread() + " pruning takes " + watch.getElapsedTimeMillis());

                Map<Long, Set<MappedNode>> queryGraphMapping = pruningAlgorithm.getQueryGraphMapping();

                queryGraphMapping.entrySet().forEach(en -> {
                    System.out.println(en.getKey() + ": " + en.getValue().size());
//                en.getValue().forEach(val -> System.out.print(val.getNodeID() + ","));
//                System.out.println();
                });

                try {
                    edAlgorithm.setStartingNode(startingNode);
                    edAlgorithm.setLabelFreq((graph).getLabelFreq());
                    edAlgorithm.setQuery(wildCardQuery);
                    edAlgorithm.setGraph(pruningAlgorithm.pruneGraph());
                    edAlgorithm.setNumThreads(this.threadsNum);
                    edAlgorithm.setQueryToGraphMap(pruningAlgorithm.getQueryGraphMapping());
                    edAlgorithm.setLimitedComputation(false);
                    edAlgorithm.setExecutionPool(ThreadPoolFactory.getWildcardSearchThreadPool());
                    edAlgorithm.compute();
                } catch (AlgorithmExecutionException e) {
                    System.out.println("testing exception" + graph);
                }
            }
            return edAlgorithm.getRelatedQueries();
        };
    }

    public void setGraphTableAlgorithm(final ComputeGraphNeighbors graphTableAlgorithm) {
        this.graphTableAlgorithm = graphTableAlgorithm;
    }
}
