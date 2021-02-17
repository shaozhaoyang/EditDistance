package eu.unitn.disi.db.grava.scc;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.BloomFilter;
import eu.unitn.disi.db.grava.utils.Utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class PathComputeRecursiveTask extends RecursiveTask<Map<Long, Set<MappedNode>>> {

    private final List<Set<MappedNode>> startingNodeMappedNodes;
    private final StopWatch total;
    private final int threshold;
    private final Map<Long, HashSet<Edge>> paths;
    private final Map<Long, Map<String, Edge>> pathPrefix;
    private final Map<Long, Map<String, Integer>> queryPaths;
    private Multigraph graph;
    private Multigraph query;
    private int k;
    private Map<Long, BloomFilter<String>> gPathTables;
    private Long startingNode;
    private ForkJoinPool forkJoinPool;

    public PathComputeRecursiveTask(final Map<Long, BloomFilter<String>> gPathTables,
                                    final Multigraph query,
                                    final Multigraph graph,
                                    final long startingNode,
                                    final int threshold,
                                    final int neighbourNum,
                                    final List<Set<MappedNode>> startingNodeMappedNodes,
                                    final Map<Long, Map<String, Edge>> pathPrefix,
                                    final Map<Long, Map<String, Integer>> queryPaths,
                                    final ForkJoinPool forkJoinPool,
                                    final StopWatch total) {
        this.startingNode = startingNode;
        this.k = neighbourNum;
        this.gPathTables = gPathTables;
        this.query = query;
        this.graph = graph;
        this.threshold = threshold;
        this.startingNodeMappedNodes = startingNodeMappedNodes;
        this.total = total;
        this.pathPrefix = pathPrefix;
        this.queryPaths = queryPaths;
        this.forkJoinPool = forkJoinPool;
        paths = new HashMap<>();
    }

    @Override
    public Map<Long, Set<MappedNode>> compute() {
        final Map<Long, Set<MappedNode>> crtQueryGraphMapping = new HashMap<>();
        if (startingNodeMappedNodes.size() > 1) {
            final boolean isEven = startingNodeMappedNodes.size() % 2 == 0;
            final List<List<Set<MappedNode>>> partitions = Lists.partition(startingNodeMappedNodes, isEven ? startingNodeMappedNodes.size() / 2 :
                    startingNodeMappedNodes.size() / 2 + 1);


            final PathComputeRecursiveTask subTask1 = new PathComputeRecursiveTask(gPathTables, query, graph, startingNode,
                    threshold, k, partitions.get(0), pathPrefix, queryPaths, forkJoinPool, total);
            final PathComputeRecursiveTask subTask2 = new PathComputeRecursiveTask(gPathTables, query, graph, startingNode,
                    threshold, k, partitions.get(1), pathPrefix, queryPaths, forkJoinPool, total);
            forkJoinPool.submit(subTask1);
            forkJoinPool.submit(subTask2);
            Map<Long, Set<MappedNode>> result1 = subTask1.join();
            Map<Long, Set<MappedNode>> result2 = subTask2.join();
            query.vertexSet().forEach(
                    queryNode ->
                            crtQueryGraphMapping.put(queryNode, Sets.union(result1.get(queryNode), result2.get(queryNode)))
            );
            System.out.println(
                    Thread.currentThread() + " completing " + startingNodeMappedNodes.size() + "："
                            + total.getElapsedTimeMillis());
        } else {
            Map<Long, Set<MappedNode>> candidateNextLevel = candidateNextLevel(startingNodeMappedNodes.get(0));

            //Long label;
            Long candidate, currentQueryNode;
            MappedNode graphCandidate;
            LinkedList<Long> queryNodeToVisit = new LinkedList<>();
            List<MappedNode> nodesToVisit;

            Map<Long, List<Long>> inQueryEdges;
            Map<Long, List<Long>> outQueryEdges;
            Set<MappedNode> mappedNodes;
            Set<Long> visitedQueryNodes = new HashSet<>();
            int i;
            //Just to try - Candidate is the first
            candidate = startingNode;
            queryNodeToVisit.add(candidate);
            Utilities.bsCount = 0;
            while (!queryNodeToVisit.isEmpty()) {

                currentQueryNode = queryNodeToVisit.poll();
                mappedNodes = crtQueryGraphMapping.get(currentQueryNode);

                //Compute the valid edges to explore and update the nodes to visit
//                medium = Utilities.bsCount;
                inQueryEdges = computeAdjacentNodes(currentQueryNode, visitedQueryNodes, queryNodeToVisit, true);
                outQueryEdges = computeAdjacentNodes(currentQueryNode, visitedQueryNodes, queryNodeToVisit, false);
//                System.out.println(medium + " " + Utilities.bsCount);
                if (candidateNextLevel.containsKey(currentQueryNode)) {

                    nodesToVisit = new ArrayList<>();
                    nodesToVisit.addAll(candidateNextLevel.get(currentQueryNode));
                    assert mappedNodes == null : String
                            .format("The current query node %d, has already been explored", currentQueryNode);
                    mappedNodes = new HashSet<>();

                    //countNodes = 0;
                    //We should check if ALL the query nodes matches and then add the node
                    for (i = 0; i < nodesToVisit.size(); i++) {
                        int testSize = nodesToVisit.size();
                        graphCandidate = nodesToVisit.get(i);

                        if (this.matchesWithPathNeighbor(graphCandidate, currentQueryNode, queryPaths, pathPrefix,
                                total)) {

                            mappedNodes.add(graphCandidate);
//                            medium = Utilities.bsCount;
                            //check if the outgoing-incoming edges matches, if yes add to the next level
                            mapNodes(graphCandidate, graph.incomingEdgesOf(graphCandidate.getNodeID()),
                                    inQueryEdges,
                                    candidateNextLevel, true);
                            mapNodes(graphCandidate, graph.outgoingEdgesOf(graphCandidate.getNodeID()),
                                    outQueryEdges,
                                    candidateNextLevel, false);

//                            System.out.println(medium + " " + Utilities.bsCount);
                        }
//                            System.out.println(
//                                    Thread.currentThread() + " " + currentQueryNode + " graph node " + graphCandidate
//                                            + " query "
//                                            + "paths takes "
//                                            + total.getElapsedTimeMillis());
                    }

                    crtQueryGraphMapping.put(currentQueryNode, mappedNodes);
                    //add the out edges to the visited ones
                    visitedQueryNodes.add(currentQueryNode);
                } else { //No map is possible anymore
                    break;
                }
            }
        }
        return crtQueryGraphMapping;
    }

    private Map<Long, Set<MappedNode>> candidateNextLevel(final Set<MappedNode> startingNodeMappedNodes) {
        final Map<Long, Set<MappedNode>> candidateNextLevel = new HashMap<>();
        for (Long node : query.vertexSet()) {
            if (startingNode.equals(node)) {
                candidateNextLevel.put(node, startingNodeMappedNodes);
            } else {
                candidateNextLevel.put(node, new HashSet<>());
            }
        }
        return candidateNextLevel;
    }

    private void mapNodes(MappedNode currentNode, Collection<Edge> graphEdges, Map<Long, List<Long>> queryEdges,
                          Map<Long, Set<MappedNode>> nextLevel, boolean incoming) {
        MappedNode nodeToAdd = null;
        List<Long> labeledNodes = null;
        List<Long> omniNodes;
        Long nodeID;
        int i;
        boolean canHaveMoreDif = true;
        omniNodes = queryEdges.get(0L);

        if (currentNode.getDist() == threshold) {
            canHaveMoreDif = false;
        }
        for (Edge gEdge : graphEdges) {
            labeledNodes = null;
            nodeID = incoming ? gEdge.getSource() : gEdge.getDestination();
            labeledNodes = queryEdges.get(gEdge.getLabel());
            if (omniNodes != null) {
                if (labeledNodes == null) {
                    labeledNodes = new ArrayList<Long>();
                }
                for (Long omniNode : omniNodes) {
                    if (!labeledNodes.contains(omniNode)) {
                        labeledNodes.add(omniNode);
                    }
                }
            }
            if (labeledNodes != null) {
                nodeToAdd = new MappedNode(nodeID, gEdge, currentNode.getDist(), !incoming, false);
//            	uptCount--;
                for (i = 0; i < labeledNodes.size(); i++) {
                    nextLevel.get(labeledNodes.get(i)).add(nodeToAdd);
                }
            }
            if (canHaveMoreDif) {
                for (Map.Entry<Long, List<Long>> entry : queryEdges.entrySet()) {
                    if (!entry.getKey().equals(gEdge.getLabel())) {
                        for (Long oneNode : entry.getValue()) {
                            nextLevel.get(oneNode)
                                    .add(new MappedNode(nodeID, gEdge, currentNode.getDist() + 1, !incoming, true));
                        }
                    }
                }
            }
        }
    }

    private boolean matchesWithPathNeighbor(MappedNode mappedGNode, long qNode,
                                            Map<Long, Map<String, Integer>> queryPaths,
                                            Map<Long, Map<String, Edge>> pathPrefix, StopWatch total) {
        BloomFilter<String> bf = gPathTables.get(mappedGNode.getNodeID());
        int count = 0;
        boolean isGood = true;
        if (queryPaths.get(qNode).size() - bf.count() > this.threshold) {
            return false;
        }
        Map<String, Edge> prefixMap = pathPrefix.get(qNode);
        Map<String, Integer> wildcardPaths = new HashMap<>();
        Set<Edge> seenPrefixEdges = new HashSet<>();

//        System.out.println(Thread.currentThread() + " " + qNode + " graph node " + mappedGNode + " query "
//                + "paths takes "
//                + total.getElapsedTimeMillis());
        for (Map.Entry<String, Integer> path : queryPaths.get(qNode).entrySet()) {
            Edge prefixEdge = prefixMap.get(path.getKey());
            String label = (prefixEdge.getSource().equals(qNode) ? "" : "-") + prefixEdge.getLabel();
            if (!seenPrefixEdges.contains(prefixEdge)) {
                String prefixTemp = label + "|" + 1;
                if (!bf.contains(prefixTemp)) {
                    int prefixCount = wildcardPaths.getOrDefault(label, 0);
                    prefixCount++;
                    wildcardPaths.put(label, prefixCount);
                    String wcPath = path.getKey().replaceFirst(String.valueOf(prefixEdge.getLabel()), "0");
                    count += checkDiff(bf, wcPath, path.getValue());
                    seenPrefixEdges.add(prefixEdge);
                } else {
                    count += checkDiff(bf, path.getKey(), path.getValue());
                }
            } else {
                String wcPath = path.getKey().replaceFirst(String.valueOf(prefixEdge.getLabel()), "0");
                int wcPathCount = wildcardPaths.getOrDefault(wcPath, 0);
                wcPathCount += path.getValue();
                wildcardPaths.put(wcPath, wcPathCount);
            }

            if (count > this.threshold) {
                return false;
            }
        }

//        System.out.println(Thread.currentThread() + " " + qNode + " path size " + queryPaths.get(qNode).size() +
//                "paths takes "
//                + total.getElapsedTimeMillis());

        for (Map.Entry<String, Integer> pathCount : wildcardPaths.entrySet()) {
            count += checkDiff(bf, pathCount.getKey(), pathCount.getValue());
            if (count > this.threshold) {
                return false;
            }
        }
        return true;
    }

    private int checkDiff(BloomFilter<String> bf, String path, int count) {
        for (int i = count; i > 0; i--) {
            String temp = path + "|" + i;
            if (!bf.contains(temp)) {
                return i;
            }
        }
        return 0;
    }

    private Map<Long, List<Long>> computeAdjacentNodes(long node, Set<Long> visitedQueryNodes,
                                                       List<Long> queryNodeToVisit, boolean incoming) {
        Collection<Edge> queryEdges =
                incoming ? query.incomingEdgesOf(node) : query.outgoingEdgesOf(node);

        List<Long> nodes;
        Map<Long, List<Long>> outMapping = new HashMap<>();
        Set<Long> toVisit = new HashSet<>();
        Long nodeToAdd;
//        double preSel = prefixSelectivities.get(node);
        double sel;
        HashSet<Edge> ps;
        if (paths.containsKey(node)) {
            ps = paths.get(node);
        } else {
            ps = new HashSet<Edge>();
        }
        for (Edge edge : queryEdges) {
            nodes = outMapping.get(edge.getLabel());
            if (nodes == null) {
                nodes = new ArrayList<>();
            }
            nodeToAdd = incoming ? edge.getSource() : edge.getDestination();
            if (!visitedQueryNodes.contains(nodeToAdd)) {
                nodes.add(nodeToAdd);
                toVisit.add(nodeToAdd);
                ps.add(edge);
            }

            outMapping.put(edge.getLabel(), nodes);
        }
        paths.put(node, ps);
        queryNodeToVisit.addAll(toVisit);
        return outMapping;
    }
}
