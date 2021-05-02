package eu.unitn.disi.db.tool;

import eu.unitn.disi.db.grava.graphs.Edge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */

/**
 * @author Zhaoyang
 *
 */
public class GraphGenerator {

    private static final long MAX_NODE_COUNT = 10000;
    private static final int MAX_DEGREE = 5;
    private static final int MIN_DEGREE_TO_BFS = 5;
    private long nodeCount = 0;

    public GraphGenerator(String fileName) throws IOException {
        final Map<Long, Set<Edge>> edgeMap = read(fileName);
        for (int i = 5; i <= 5; i += 5) {
            bfs(edgeMap, i);
        }
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
//		String a = "<http://rdf.freebase.com/ns/award.award_winner>";
//		System.out.println(a.split("/")[a.split("/").length-1]);
        GraphGenerator rf = new GraphGenerator("graph/1000000nodes-sout.graph");
//		String a = "<http://rdf.freebase.com/ns/american_football.football_player.footballdb_id>    <http://www.w3
//		.org/2000/01/rdf-schema#label>    \"footballdb ID\"@en      .";
//		System.out.println(a.split(" ")[2]);
    }

    private Map<Long, Set<Edge>> read(String fileName) throws IOException {
        FileInputStream fin = new FileInputStream(fileName);
        InputStreamReader xover = new InputStreamReader(fin);
        BufferedReader is = new BufferedReader(xover);
        String line;
        // Now read lines of text: the BufferedReader puts them in lines,
        // the InputStreamReader does Unicode conversion, and the
        // GZipInputStream "gunzip"s the data from the FileInputStream.

        Map<Long, Set<Edge>> edgeMap = new HashMap<>();
        long crtNodeId;
        try {
            int count = 0;

            while ((line = is.readLine()) != null) {
                String[] words = line.split(" ");
                if (words.length < 3) {
                    continue;
                }
                Long subjectId = Long.parseLong(words[0]);
                Long objectId = Long.parseLong(words[1]);
                Long predicateId = Long.parseLong(words[2]);
                crtNodeId = subjectId;
                Set<Edge> edges = edgeMap.getOrDefault(crtNodeId, new HashSet<>());

                edges.add(new Edge(subjectId, objectId, predicateId));

                edgeMap.put(crtNodeId, edges);

                count++;
                if (count % 100000 == 0) {
                    System.out.println("Processed " + count);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (is != null) {
                is.close();
            }
        }
        int count = 0;
        for (Map.Entry<Long, Set<Edge>> entry : edgeMap.entrySet()) {
            count += entry.getValue().size();
        }
        return edgeMap;
    }

    private long totalDegree(Map<Long, Set<Edge>> edgeMap) {
        long totalDegree = 0;
        for (Map.Entry<Long, Set<Edge>> entry : edgeMap.entrySet()) {
            totalDegree += entry.getValue().size() * 2L;
        }
        return totalDegree;
    }

    private void bfs(final Map<Long, Set<Edge>> edgeMap, final int maxDegree) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(+MAX_NODE_COUNT + "nodes-sout" + "-d" + maxDegree +
                ".graph",
                false));

        Set<Long> allNodes = new HashSet<>();
        Set<Long> visited = new HashSet<>();
        Queue<Long> toVisit = new LinkedList<>();
        Map<Long, Set<Edge>> highDegreeNodes = new HashMap<>();

        long maxDegreeNode = edgeMap.entrySet().iterator().next().getKey();
        long crtMaxDegree = 0;
        for (Map.Entry<Long, Set<Edge>> entry : edgeMap.entrySet()) {
            if (entry.getValue().size() > crtMaxDegree) {
                maxDegreeNode = entry.getKey();
                crtMaxDegree = entry.getValue().size();
            }
        }
        toVisit.add(maxDegreeNode);

        final Map<Long, Integer> countMap = new HashMap<>();
        long crtDegree = 0;

        while (allNodes.size() < MAX_NODE_COUNT) {
            do {
                Long crtNode = toVisit.poll();
                if (edgeMap.get(crtNode) != null) {
                    if (visited.contains(crtNode)) {
                        continue;
                    }
                    Set<Edge> edges = edgeMap.get(crtNode);

                    Queue<Edge> sortedEdges = new PriorityQueue<>(new Comparator<Edge>() {
                        @Override
                        public int compare(final Edge o1, final Edge o2) {
                            int d1 = Optional.ofNullable(edgeMap.get(o1.getDestination())).map(Set::size).orElse(0);
                            int d2 = Optional.ofNullable(edgeMap.get(o2.getDestination())).map(Set::size).orElse(0);
                            return d2 - d1;
                        }
                    }
                    );

                    sortedEdges.addAll(edges);

                    List<Edge> usedEdges = new LinkedList<>();
                    int count = 0;
                    visited.add(crtNode);
                    while (!sortedEdges.isEmpty()) {
                        Edge edge = sortedEdges.poll();
                        int sourceCount = countMap.getOrDefault(edge.getSource(), 0);
                        sourceCount++;

                        int dstCount = countMap.getOrDefault(edge.getDestination(), 0);
                        dstCount++;


                        if ((crtDegree / visited.size()) <= maxDegree) {
                            usedEdges.add(edge);

                            countMap.put(edge.getSource(), sourceCount);
                            countMap.put(edge.getDestination(), dstCount);
                            count++;
                            crtDegree += 1;
                        } else {
                            break;
                        }
                    }
                    if (!sortedEdges.isEmpty()) {
                        Set<Edge> restEdges = new HashSet<>();
                        restEdges.addAll(sortedEdges);
                        highDegreeNodes.put(crtNode, restEdges);
                    }
                    List<Long> nodes = usedEdges.stream()
                            .map(Edge::getDestination)
                            .filter(node -> !visited.contains(node))
                            .collect(Collectors.toList());
                    usedEdges.stream()
                            .map(Edge::getDestination)
                            .forEach(allNodes::add);
                    usedEdges.stream()
                            .map(Edge::getSource)
                            .forEach(allNodes::add);
                    write(writer, usedEdges);
                    toVisit.addAll(nodes);
//                }
                    edgeMap.remove(crtNode);
                    edges = null;
                    nodes = null;
                }
            } while ((crtDegree / visited.size()) > maxDegree);

            for (Map.Entry<Long, Set<Edge>> entry: highDegreeNodes.entrySet()) {
                Iterator<Edge> iterator = entry.getValue().iterator();
                List<Edge> usedEdges = new LinkedList<>();
                while (iterator.hasNext()) {
                    if ((crtDegree / visited.size()) < maxDegree && allNodes.size() < MAX_NODE_COUNT) {
                        Edge element = iterator.next();
                        usedEdges.add(element);
                        toVisit.add(element.getDestination());
                        allNodes.add(element.getDestination());
                        crtDegree += 1;
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                write(writer, usedEdges);
                if ((crtDegree / allNodes.size()) > maxDegree ||  allNodes.size() < MAX_NODE_COUNT) {

                    break;
                }
            }
            highDegreeNodes.entrySet().removeIf(e -> e.getValue().isEmpty());
        }
//        System.out.println(allNodes.size());
        System.out.println("avg degree:" + crtDegree / (visited.size()));
        writer.flush();
        writer.close();
    }

    private void write(final BufferedWriter writer, final List<Edge> edges) throws IOException {
        for (Edge e : edges) {
            writer.write(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
            writer.newLine();
        }
        writer.flush();
    }
}
