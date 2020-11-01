/*
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package eu.unitn.disi.db.exemplar.core.algorithms.steps;

import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.exemplar.core.IsomorphicQuery;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.algorithms.IsomorphicQuerySearch;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.LabelContainer;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.Utilities;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class GraphIsomorphismRecursiveStep extends AlgorithmStep<RelatedQuery> {

    private final Long queryConcept;
    private int searchCount;
    private Map<Long, LabelContainer> labelFreq;
    private Map<Long, Set<MappedNode>> queryToGraphMap;
    private Map<Long, Set<Long>> queryToRawGraphNodes;
    private boolean isQuit;
    private int chunkSize;

    public GraphIsomorphismRecursiveStep(int threadNumber, Iterator<MappedNode> kbConcepts, Long queryConcept,
                                         Multigraph query, Multigraph targetSubgraph, boolean limitComputation,
                                         boolean skipSave, int chunkSize, Map<Long, Set<MappedNode>> queryToGraphMap,
                                         Map<Long, LabelContainer> labelFreq) {
        super(threadNumber, kbConcepts, query, targetSubgraph, limitComputation, skipSave);
        this.queryConcept = queryConcept;
        this.chunkSize = chunkSize;
        this.queryToGraphMap = queryToGraphMap;
        this.labelFreq = labelFreq;
        this.queryToRawGraphNodes = queryToGraphMap.entrySet()
                .stream()
                .collect(Collectors
                        .toMap(Entry::getKey, entry -> entry.getValue().stream().map(MappedNode::getNodeID).collect(
                                Collectors.toSet())));
    }

    //todo: sort by frequency
    public static List<Edge> sortEdge(final Set<Edge> edges, final Map<Long, LabelContainer> labelFreq) {
        List<Edge> sortedEdges = new ArrayList<>();
        PriorityQueue<Edge> pq = new PriorityQueue<>(new Comparator<Edge>() {
            public int compare(Edge e1, Edge e2) {
                if (e1.getLabel().equals(0L)) {
                    return 1;
                } else if (e2.getLabel().equals(0L)) {
                    return -1;
                } else {
                    return (int) (labelFreq.get(e1.getLabel()).getFrequency() - labelFreq.get(e2.getLabel())
                            .getFrequency());
                }
            }
        });
        for (Edge qe : edges) {
            pq.add(qe);
        }
        while (!pq.isEmpty()) {
            sortedEdges.add(pq.poll());
        }
        return sortedEdges;
    }

    /**
     * Checks if, for a given node, it exist an <b>outgoing</b> with that label and returns all the graphEdges found
     *
     * @param label the label we are looking for
     * @param graphEdges the knoweldgebase graphEdges
     * @return labeled graphEdges, can be empty
     */
    public static List<Edge> findEdges(Long label, Collection<Edge> graphEdges) {

        // Compare to the graphEdges in the KB exiting from the mapped node passed
        List<Edge> edges = new ArrayList<>();

        for (Edge Edge : graphEdges) {
            if (label == Edge.GENERIC_EDGE_LABEL || Edge.getLabel().longValue() == label || label == 0L) {
                edges.add(Edge);
            }
        }
        return edges;
    }

    @Override
    public List<RelatedQuery> call() throws Exception {
        IsomorphicQuery relatedQuery;
        List<IsomorphicQuery> relatedQueriesPartial = new LinkedList<>();
        Set<RelatedQuery> relatedQueries = new HashSet<>();
//        searchCount = 0;
        boolean warned = false;
        watch.start();
        int i = 0;
        this.isQuit = false;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        while (graphNodes.hasNext()) {
//        	System.out.println("Thread " + threadNumber + " Finshed " + ((double)i / chunkSize) * 100 + "%");
            MappedNode node = graphNodes.next();
//        	System.out.println("Processing node " + node.getNodeID());
//        	System.out.println(node);
            i++;
            try {
                relatedQuery = new IsomorphicQuery(query);
                //Map the first node
                relatedQuery.map(queryConcept, node);
                Set<Long> visited = new HashSet<>();
                visited.add(queryConcept);
                relatedQueriesPartial = createQueries(query, queryConcept, node, relatedQuery, 0, visited);
//                System.out.println(Utilities.searchCount);
                if (this.isQuit) {
                    break;
                }
                if (relatedQueriesPartial != null) {
                    if (skipSave) {
                        continue;
                    }
                    IsomorphicQuerySearch.answerCount += relatedQueriesPartial.size();
                    relatedQueries.addAll(relatedQueriesPartial);
//                    for (RelatedQuery rq : relatedQueries){
//                    	System.out.println(rq);
//                    }
                    if (watch.getElapsedTimeMillis() > WARN_TIME || IsomorphicQuerySearch.answerCount > MAX_RELATED) {
//                        warn("More than " + MAX_RELATED + " partial isomorphic results");
                        warned = true;
                        if (limitComputation) {
//                            warn("Computation interrupted after " + IsomorphicQuerySearch.answerCount + " partial isomorphic results");
                            break;
                        }
//                        IsomorphicQuerySearch.answerCount = 0;
                        relatedQueriesPartial.clear();
                        IsomorphicQuerySearch.isBad = true;
                    }

//                    if ((!warned && watch.getElapsedTimeMillis() > WARN_TIME && IsomorphicQuerySearch.answerCount > MAX_RELATED)) {
//                        warn("More than " + MAX_RELATED + " partial isomorphic results");
//                        warned = true;
//                        if (limitComputation) {
//                            warn("Computation interrupted after " + IsomorphicQuerySearch.answerCount + " partial isomorphic results");
//                            break;
//                        }
//                        IsomorphicQuerySearch.isBad = true;
//                    }
                }
//                System.out.println(Utilities.searchCount);
            } catch (OutOfMemoryError E) {
                if (relatedQueriesPartial != null) {
                    relatedQueriesPartial.clear();
                }
                IsomorphicQuerySearch.isBad = true;
                error("Memory exausted, so we are returning something but not everything.");
                System.gc();
                return new LinkedList<>();
            }

            //if (watch.getElapsedTimeMillis() > WARN_TIME) {
            //    info("Computation %d [%d] took %d ms", threadNumber, Thread.currentThread().getId(), watch.getElapsedTimeMillis());
            //}
            System.out.println(node + " takes " + stopWatch.getElapsedTimeSecs());
            System.out.println("Answer size:" + relatedQueries.size());
        }
        watch.stop();
        return new LinkedList<>(relatedQueries);
    }

    /**
     * Given a query, a starting node from the query, and a node from the knowledgeBase , tries to build up a related
     * query
     */
    public List<IsomorphicQuery> createQueries(Multigraph query, Long queryNode, MappedNode graphNode,
                                               IsomorphicQuery relatedQuery, int depth, Set<Long> visited) {
        // Initialize the queries set
        //Given the current situation we expect to build more than one possible related query
        List<IsomorphicQuery> relatedQueries = new ArrayList<>();
        relatedQueries.add(relatedQuery);

        // The graphEdges exiting from the query node passed
        Collection<Edge> queryEdgesOut = query.outgoingEdgesOf(queryNode);
        // The graphEdges entering the query node passed
        Collection<Edge> queryEdgesIn = query.incomingEdgesOf(queryNode);

        // The graphEdges in the KB exiting from the mapped node passed
        Collection<Edge> graphEdgesOut = graph.outgoingEdgesOf(graphNode.getNodeID());
        // The graphEdges in the KB entering the mapped node passed
        Collection<Edge> graphEdgesIn = graph.incomingEdgesOf(graphNode.getNodeID());
//        System.out.println(graphNode.getNodeID() + " " + (graphEdgesOut.size() + graphEdgesIn.size()));

        // Null handling
        queryEdgesIn = queryEdgesIn == null ? new HashSet<Edge>() : queryEdgesIn;
        queryEdgesOut = queryEdgesOut == null ? new HashSet<Edge>() : queryEdgesOut;
        graphEdgesIn = graphEdgesIn == null ? new HashSet<Edge>() : graphEdgesIn;
        graphEdgesOut = graphEdgesOut == null ? new HashSet<Edge>() : graphEdgesOut;

        //debug("TEst %d map to  %d", queryNode, graphNode);

        //Optimization: if the queryEdges are more than the kbEdges, we are done, not isomorphic!
        if (queryEdgesIn.size() > graphEdgesIn.size() || queryEdgesOut.size() > graphEdgesOut.size()) {
            return null;
        }

        //All non mapped graphEdges from the query are put in one set
        Set<Edge> queryEdges = new HashSet<>();

        for (Edge edgeOut : queryEdgesOut) {
            if (!relatedQuery.hasMapped(edgeOut)) {
                queryEdges.add(edgeOut);
            }
        }

        for (Edge edgeIn : queryEdgesIn) {
            if (!relatedQuery.hasMapped(edgeIn)) {
                queryEdges.add(edgeIn);
            }
        }

        queryEdgesIn = null;
        queryEdgesOut = null;
        List<Edge> sortedEdges = sortEdge(queryEdges, this.labelFreq);
        //Look if we can map all the outgoing/ingoing graphEdges of the query node
//        System.out.println("=====================");
//        System.out.println("query node:" + queryNode + " graph node:" + graphNode + " depth:" + depth);
        for (Edge queryEdge : sortedEdges) {
//        	System.out.println("Processs query edge: " + queryEdge);
            if (relatedQueries.size() > MAX_RELATED) {
                System.out.println("More than " + MAX_RELATED);
                return relatedQueries;
            }
//        	System.out.println(queryEdge);
//            info("Trying to map the edge " + queryEdge);
            List<IsomorphicQuery> newRelatedQueries = new ArrayList<>();
            LinkedList<IsomorphicQuery> toTestRelatedQueries = new LinkedList<>();

            for (IsomorphicQuery current : relatedQueries) {
                if (current.hasMapped(queryEdge)) {
                    newRelatedQueries.add(current);
                } else {
                    toTestRelatedQueries.add(current);
                }
            }
            relatedQueries.clear();
            IsomorphicQuerySearch.interNum = Math.max(toTestRelatedQueries.size(), IsomorphicQuerySearch.interNum);
            // reset, we do not want too many duplicates
            relatedQueries = new LinkedList<>();

            // If all candidated have this QueryEdge mapped, go to next
            if (toTestRelatedQueries.isEmpty()) {
                relatedQueries = newRelatedQueries;
                continue;
            }

            // The label we are looking for
            Long label = queryEdge.getLabel();

            //is it isIncoming or outgoing ?
            boolean isIncoming = queryEdge.getDestination().equals(queryNode);

            List<Edge> graphEdges;
            // Look for graphEdges with the same label and same direction as the one from the query
            if (isIncoming) {
                graphEdges = findEdges(label, graphEdgesIn);
            } else {
                graphEdges = findEdges(label, graphEdgesOut);
            }

            //loggable.debug("Matching with %d graphEdges", graphEdges.size() );
            // Do we found any?
            if (graphEdges.isEmpty()) {
                // If we cannot map graphEdges, this path is wrong
                return null;
            } else {
                //Cycle through all the possible graphEdges options,
                //they would be possibly different related queries
                for (Edge graphEdge : graphEdges) {
//                	System.out.println("query edge:" + queryEdge + " graph edge: " + graphEdge);
                    //Cycle through all the possible related queries retrieved up to now
                    //A new related query is good if it finds a match
                    Long queryNextNode;
                    MappedNode graphNextNode;
                    if (isIncoming) {
                        queryNextNode = queryEdge.getSource();
                    } else {
                        queryNextNode = queryEdge.getDestination();
                    }

                    boolean graphEdgeIsIncoming = graphEdge.getDestination().equals(graphNode.getNodeID());
                    Long graphNextNodeLong =
                            graphEdge.getSource().equals(graphNode.getNodeID()) ? graphEdge.getDestination()
                                    : graphEdge.getSource();
                    if (isIncoming ^ graphEdgeIsIncoming) {
                        continue;
                    }
                    if (!queryToRawGraphNodes.get(queryNextNode).contains(graphNextNodeLong)) {
                        continue;
                    }
                    graphNextNode = new MappedNode(graphNextNodeLong, graphEdge, 0, graphEdgeIsIncoming, false);

                    for (IsomorphicQuery tempRelatedQuery : toTestRelatedQueries) {
                        if (newRelatedQueries.size() > MAX_RELATED || watch.getElapsedTimeMillis() > QUIT_TIME) {
                            if (newRelatedQueries.size() > MAX_RELATED) {
                                System.out.println("more than  partial results" + MAX_RELATED);
                            } else {
                                System.out.println("time exceeds" + QUIT_TIME);
                            }
                            this.isQuit = true;
                            return relatedQueries.size() > 0 ? relatedQueries : null;
                        }
                        if (tempRelatedQuery.isUsing(graphEdge) || tempRelatedQuery
                                .isMappedAsDifferentNode(queryNextNode, graphNextNode)) {
                            //Ok this option is already using this edge,
                            //not a good choice go away
                            //it means that this query didn't found his match in this edge
                            continue;
                        }
                        Utilities.searchCount++;
                        //Otherwise this edge can be mapped to the query edge if all goes well
                        IsomorphicQuery newRelatedQuery = tempRelatedQuery.getClone();

                        //check nodes similarity
                        //double nodeSimilarity = 0;
                        //if (isIncoming) {
                        //    nodeSimilarity = RelatedQuerySearch.conceptSimilarity(queryEdge.getSource(), graphEdge.getSource());
                        //} else {
                        //    nodeSimilarity = RelatedQuerySearch.conceptSimilarity(queryEdge.getDestination(), graphEdge.getDestination());
                        //}
                        //If the found edge peudo-destination is similar to the query edge pseudo-destination
                        //if (nodeSimilarity > RelatedQuerySearch.MIN_SIMILARITY) {
                        //The destination if outgoing the source if isIncoming

                        //Is this node coeherent with the structure?
                        if (edgeMatch(queryEdge, graphEdge, newRelatedQuery)) {
                            //That's a good edge!! Add it to this related query
                            newRelatedQuery.map(queryEdge, graphEdge);

                            //Map also the node
                            newRelatedQuery.map(queryNextNode, graphNextNode);

                            //The query node that we are going to map
                            //Does it have graphEdges that we don't have mapped?
                            boolean needExpansion = false;
                            Collection<Edge> pseudoOutgoingEdges = query.incomingEdgesOf(queryNextNode);
                            Long queryPrevNode = null;
                            if (pseudoOutgoingEdges.size() > 0) {
                                for (Edge pseudoEdge : pseudoOutgoingEdges) {
                                    needExpansion =
                                            !newRelatedQuery.hasMapped(pseudoEdge) && !pseudoEdge.equals(queryEdge);
                                    queryPrevNode =
                                            pseudoEdge.getDestination().equals(queryNextNode) ? pseudoEdge.getSource()
                                                    : pseudoEdge.getDestination();
                                    needExpansion = needExpansion && !queryPrevNode.equals(queryNode);
                                    if (needExpansion) {
                                        break;
                                    }
                                }
                            }

                            pseudoOutgoingEdges = query.outgoingEdgesOf(queryNextNode);
                            if (!needExpansion && pseudoOutgoingEdges.size() > 0) {
                                for (Edge pseudoEdge : pseudoOutgoingEdges) {
                                    needExpansion =
                                            !newRelatedQuery.hasMapped(pseudoEdge) && !pseudoEdge.equals(queryEdge);
                                    queryPrevNode =
                                            pseudoEdge.getDestination().equals(queryNextNode) ? pseudoEdge.getSource()
                                                    : pseudoEdge.getDestination();
                                    needExpansion = needExpansion && !queryPrevNode.equals(queryNode);
                                    if (needExpansion) {
                                        break;
                                    }
                                }
                            }

                            if (visited.contains(queryNextNode)) {
                                needExpansion = false;
                            }

                            //Lookout! We need to check the outgoing part, if we did not already
                            if (needExpansion) {
                                // Possible outgoing branches
                                List<IsomorphicQuery> tmpRelatedQueries;
                                //Go find them!
                                //log("Go find mapping for: " + queryNextNode + " // " + graphNextNode);
                                visited.add(queryNextNode);
                                tmpRelatedQueries = createQueries(query, queryNextNode, graphNextNode, newRelatedQuery,
                                        depth + 1, visited);
                                visited.remove(queryNextNode);
                                //Did we find any?
                                if (tmpRelatedQueries != null) {
                                    //Ok so we found some, they are all good to me
                                    //More possible related queries
                                    //They already contain the root
                                    for (IsomorphicQuery branch : tmpRelatedQueries) {
                                        //All these related queries have found in this edge their match
                                        newRelatedQueries.add(branch);
                                    }
                                }
                                // else {
                                // This query didn't find in this edge its match
                                // continue;
                                //}
                            } else {
                                //log("Complete query " + relatedQuery);
                                //this related query has found in this edge is map
                                //newRelatedQuery.map(queryNextNode, graphNextNode);
                                newRelatedQueries.add(newRelatedQuery);
                            }
                        }
                        //else {
                        //info("Edge does not match  %s   -  for %s  : %d", graphEdge.getId(), FreebaseConstants.convertLongToMid(graphNode), graphNode);
                        //}
                    }
                }
            }
            //after this cycle we should have found some, how do we check?
            if (newRelatedQueries.isEmpty()) {
                return null;
            } else {
                //basically in the *new* list are the related queries still valid and growing
                relatedQueries = newRelatedQueries;
            }

        }
        return relatedQueries.size() > 0 ? relatedQueries : null;
    }

    /**
     *
     */
    protected boolean edgeMatch(Edge queryEdge, Edge graphEdge, IsomorphicQuery r) {

        if (queryEdge.getLabel() != 0L && queryEdge.getLabel() != graphEdge.getLabel().longValue()) {
            return false;
        }

        Long querySource = null;
        Long queryDestination = null;
        Long graphSource = null;
        Long graphDestination = null;

        if (r != null) {
            if (r.isUsing(graphEdge)) {
                return false;
            }

            querySource = queryEdge.getSource();
            graphSource = graphEdge.getSource();

            boolean mappedSource = r.hasMapped(querySource);
            boolean usingSource = r.isUsing(graphSource);

            if (usingSource && !mappedSource) {
                return false;
            }

            queryDestination = queryEdge.getDestination();
            graphDestination = graphEdge.getDestination();

            boolean mappedDestination = r.hasMapped(queryDestination);
            boolean usingDestination = r.isUsing(graphDestination);
            if (usingDestination && !mappedDestination) {
                return false;
            }

            if (mappedSource && !graphSource.equals(r.isomorphicMapOf(querySource).getNodeID())) {
                return false;
            }

            if (mappedDestination && !graphDestination.equals(r.isomorphicMapOf(queryDestination).getNodeID())) {
                return false;
            }

            if (usingSource && !r.mappedAs(graphSource).equals(querySource)) {
                return false;
            }

            if (usingDestination && !r.mappedAs(graphDestination).equals(queryDestination)) {
                return false;
            }

        }
        return true;
    }
}
