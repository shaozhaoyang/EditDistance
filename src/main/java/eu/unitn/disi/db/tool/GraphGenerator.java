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
        final Map<Long, List<Edge>> edgeMap = read(fileName);
        final long totalDegree = totalDegree(edgeMap);
        for (int i = 5; i <= 25; i += 5) {
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
        GraphGenerator rf = new GraphGenerator("graph/100000nodes-sout.graph");
//		String a = "<http://rdf.freebase.com/ns/american_football.football_player.footballdb_id>    <http://www.w3.org/2000/01/rdf-schema#label>    \"footballdb ID\"@en      .";
//		System.out.println(a.split(" ")[2]);
    }

    private Map<Long, List<Edge>> read(String fileName) throws IOException {
        FileInputStream fin = new FileInputStream(fileName);
        InputStreamReader xover = new InputStreamReader(fin);
        BufferedReader is = new BufferedReader(xover);
        String line;
        // Now read lines of text: the BufferedReader puts them in lines,
        // the InputStreamReader does Unicode conversion, and the
        // GZipInputStream "gunzip"s the data from the FileInputStream.

        Map<Long, List<Edge>> edgeMap = new HashMap<>();
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
                List<Edge> edges = edgeMap.getOrDefault(crtNodeId, new LinkedList<>());

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
        return edgeMap;
    }

    private long totalDegree(Map<Long, List<Edge>> edgeMap) {
        long totalDegree = 0;
        for (Map.Entry<Long, List<Edge>> entry: edgeMap.entrySet()) {
            totalDegree += entry.getValue().size() * 2L;
        }
        return totalDegree;
    }

    private void bfs(final Map<Long, List<Edge>> edgeMap, final int maxDegree) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter( + MAX_NODE_COUNT + "nodes-sout" + "-d" + maxDegree +
                ".graph",
                false));

        Set<Long> allNodes = new HashSet<>();
        Set<Long> visited = new HashSet<>();
        Queue<Long> toVisit = new LinkedList<>();
        Queue<Long> moreCandidates = new LinkedList<>();

        long minDegreeNode = edgeMap.entrySet().iterator().next().getKey();
        long minDegree = Integer.MAX_VALUE;
        for (Map.Entry<Long, List<Edge>> entry : edgeMap.entrySet()) {
            if (entry.getValue().size() < minDegree) {
                minDegreeNode = entry.getKey();
                minDegree = entry.getValue().size();
            }
        }
        toVisit.add(minDegreeNode);

        final Map<Long, Integer> countMap = new HashMap<>();
        long crtDegree = 0;

        while ((!toVisit.isEmpty() || !moreCandidates.isEmpty()) && allNodes.size() < MAX_NODE_COUNT) {
            Long crtNode = null;
            if (!toVisit.isEmpty()) {
                crtNode = toVisit.poll();
            } else {
                crtNode = moreCandidates.poll();
            }

            if (edgeMap.get(crtNode) != null) {
                if (visited.contains(crtNode)) {
                    continue;
                }
                List<Edge> edges = edgeMap.get(crtNode);
                Queue<Edge> sortedEdges = new PriorityQueue<>(new Comparator<Edge>() {
                    @Override
                    public int compare(final Edge o1, final Edge o2) {
                        int d1 = Optional.ofNullable(edgeMap.get(o1.getDestination())).map(List::size).orElse(0);
                        int d2 = Optional.ofNullable(edgeMap.get(o2.getDestination())).map(List::size).orElse(0);
                        return d2 - d1;
                    }
                }
                );
                sortedEdges.addAll(edges);

                List<Edge> usedEdges = new LinkedList<>();
                int count = 0;
                for (Edge edge : sortedEdges) {
                    int sourceCount = countMap.getOrDefault(edge.getSource(), 0);
                    sourceCount++;

                    int dstCount = countMap.getOrDefault(edge.getDestination(), 0);
                    dstCount++;


                    if (crtDegree/(allNodes.size() + 1) <= MAX_DEGREE) {
                        usedEdges.add(edge);

                        countMap.put(edge.getSource(), sourceCount);
                        countMap.put(edge.getDestination(), dstCount);
                        count++;
                        crtDegree += 2;
                        if (count > MAX_DEGREE) {
                            break;
                        }
                    } else {
                        if (!visited.contains(edge.getDestination())) {
                            moreCandidates.add(edge.getDestination());
                        }
                    }
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
                long crtAvg = crtDegree/allNodes.size();
//                }
                visited.add(crtNode);
                edgeMap.remove(crtNode);
                edges = null;
                nodes = null;
            }
        }
//        System.out.println(allNodes.size());
        System.out.println("avg degree:" + crtDegree/(allNodes.size()));
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
