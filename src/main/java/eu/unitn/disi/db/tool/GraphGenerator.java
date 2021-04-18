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
    private static final int MAX_DEGREE = 15;
    private long nodeCount = 0;

    public GraphGenerator(String fileName) throws IOException {
        process(fileName);
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
//		String a = "<http://rdf.freebase.com/ns/award.award_winner>";
//		System.out.println(a.split("/")[a.split("/").length-1]);
        GraphGenerator rf = new GraphGenerator("freebase-sout.graph");
//		String a = "<http://rdf.freebase.com/ns/american_football.football_player.footballdb_id>    <http://www.w3.org/2000/01/rdf-schema#label>    \"footballdb ID\"@en      .";
//		System.out.println(a.split(" ")[2]);
    }

    private void process(String fileName) throws IOException {
        // Since there are 4 constructor calls here, I wrote them out in full.
        // In real life you would probably nest these constructor calls.
        FileInputStream fin = new FileInputStream(fileName);
        InputStreamReader xover = new InputStreamReader(fin);
        BufferedReader is = new BufferedReader(xover);
        BufferedWriter bw = new BufferedWriter(new FileWriter("graph_" + MAX_NODE_COUNT + ".txt", false));
        String line;
        // Now read lines of text: the BufferedReader puts them in lines,
        // the InputStreamReader does Unicode conversion, and the
        // GZipInputStream "gunzip"s the data from the FileInputStream.

        Map<Long, Queue<Edge>> edgeMap = new HashMap<>();
        Queue<Long> queue = new PriorityQueue<>(Long::compareTo);
        queue.add(48L);
        long crtNodeId = 48L;
        Set<Long> visited = new HashSet<>();
        Set<Long> allNodes = new HashSet<>();
        Map<Long, Integer> countMap = new HashMap<>();
        try {
            int count = 0;

            while ((line = is.readLine()) != null && allNodes.size() < MAX_NODE_COUNT) {
                String[] words = line.split(" ");
                if (words.length < 3) {
                    continue;
                }
                Queue<Edge> edges = edgeMap.getOrDefault(crtNodeId, new PriorityQueue<>(
                        Comparator.comparingLong(Edge::getDestination).reversed()));

                Long subjectId = Long.parseLong(words[0]);
                Long objectId = Long.parseLong(words[1]);
                Long predicateId = Long.parseLong(words[2]);

                edges.add(new Edge(subjectId, objectId, predicateId));
//                if (edges.size() > MAX_DEGREE * 2) {
//                    edges.poll();
//                }

                edgeMap.put(crtNodeId, edges);

                count++;
                if (count % 100000 == 0) {
                    System.out.println(
                            "Processed " + count + " lines; " + " nodes count " + allNodes.size() + "; crtNode "
                                    + crtNodeId + "; queue "
                                    + "peek " + queue.peek() + " queue size:" + queue.size());
                }
                if (subjectId.equals(crtNodeId)) {
                    continue;
                } else if (crtNodeId == queue.peek()) {
                    bfs(bw, edgeMap, queue, visited, allNodes, countMap);
                } else if (crtNodeId > queue.peek()) {
                    queue.poll();
                }
                crtNodeId = subjectId;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            bw.flush();
            if (is != null) {
                is.close();
            }
            if (bw != null) {
                bw.flush();
                bw.close();
            }
        }
    }

    private void bfs(final BufferedWriter writer, final Map<Long, Queue<Edge>> edgeMap, final Queue<Long> queue,
                     final Set<Long> visited, final Set<Long> allNodes, final Map<Long, Integer> countMap) throws IOException {
        while (!queue.isEmpty() && allNodes.size() < MAX_NODE_COUNT) {
            if (edgeMap.get(queue.peek()) != null) {
                Long crtNode = queue.poll();
                if (visited.contains(crtNode)) {
                    continue;
                }
                Queue<Edge> edges = edgeMap.get(crtNode);
                List<Edge> usedEdges = new LinkedList<>();
                int count = 0;
                for (Edge edge : edges) {
                    int sourceCount = countMap.getOrDefault(edge.getSource(), 0);
                    sourceCount++;

                    int dstCount = countMap.getOrDefault(edge.getDestination(), 0);
                    dstCount++;


                    countMap.put(edge.getSource(), sourceCount);
                    countMap.put(edge.getDestination(), dstCount);

                    if (sourceCount <= MAX_DEGREE && dstCount <= MAX_DEGREE) {
                        usedEdges.add(edge);
                        count++;
                        if (count > MAX_DEGREE) {
                            break;
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
//                if (edges.size() >= MAX_DEGREE) {
//                	write(writer, edges.subList(0, MAX_DEGREE));
//                    queue.addAll(nodes.subList(0, MAX_DEGREE));
//                } else {
                write(writer, usedEdges);
                queue.addAll(nodes);
//                }
                visited.add(crtNode);
                edgeMap.remove(crtNode);
                edges = null;
                nodes = null;
            } else {
                break;
            }
        }
    }

    private void write(final BufferedWriter writer, final List<Edge> edges) throws IOException {
        for (Edge e : edges) {
            writer.write(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
            writer.newLine();
        }
        writer.flush();
    }
}
