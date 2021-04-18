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
    private static final int MAX_DEGREE = 5;
    private static final int MIN_DEGREE_TO_BFS = 5;
    private long nodeCount = 0;

    public GraphGenerator(String fileName) throws IOException {
        final Map<Long, Queue<Edge>> edgeMap = read(fileName);
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

    private Map<Long, Queue<Edge>> read(String fileName) throws IOException {
        FileInputStream fin = new FileInputStream(fileName);
        InputStreamReader xover = new InputStreamReader(fin);
        BufferedReader is = new BufferedReader(xover);
        String line;
        // Now read lines of text: the BufferedReader puts them in lines,
        // the InputStreamReader does Unicode conversion, and the
        // GZipInputStream "gunzip"s the data from the FileInputStream.

        Map<Long, Queue<Edge>> edgeMap = new HashMap<>();
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
                Queue<Edge> edges = edgeMap.getOrDefault(crtNodeId, new PriorityQueue<>(
                        Comparator.comparingLong(Edge::getDestination)));

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

    private void process(String fileName, int maxDegree) throws IOException {
        // Since there are 4 constructor calls here, I wrote them out in full.
        // In real life you would probably nest these constructor calls.
        FileInputStream fin = new FileInputStream(fileName);
        InputStreamReader xover = new InputStreamReader(fin);
        BufferedReader is = new BufferedReader(xover);
        BufferedWriter bw = new BufferedWriter(new FileWriter( + MAX_NODE_COUNT + "nodes-sout" + "-d" + maxDegree +
                ".graph",
                false));
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
                        Comparator.comparingLong(Edge::getDestination)));

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
//                if (subjectId.equals(crtNodeId)) {
//                    continue;
//                } else if (crtNodeId == queue.peek()) {
//                    bfs(bw, edgeMap, queue, visited, allNodes, countMap);
//                } else if (crtNodeId > queue.peek()) {
//                    bfs(bw, edgeMap, queue, visited, allNodes, countMap);
//                }
//                crtNodeId = subjectId;
            }
            bfs(bw, edgeMap, queue, visited, allNodes, countMap);
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

    private void bfs(final Map<Long, Queue<Edge>> edgeMap, final int maxDegree) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter( + MAX_NODE_COUNT + "nodes-sout" + "-d" + maxDegree +
                ".graph",
                false));

        Set<Long> allNodes = new HashSet<>();
        Set<Long> visited = new HashSet<>();
        Queue<Long> toVisit = new LinkedList<>();


        long maxDegreeNode = edgeMap.entrySet().iterator().next().getKey();
        long crtMaxDegree = 0;
        for (Map.Entry<Long, Queue<Edge>> entry : edgeMap.entrySet()) {
            if (entry.getValue().size() > crtMaxDegree) {
                maxDegreeNode = entry.getKey();
                crtMaxDegree = entry.getValue().size();
            }
        }
        toVisit.add(maxDegreeNode);

        final Map<Long, Integer> countMap = new HashMap<>();

        while (!toVisit.isEmpty() && allNodes.size() < MAX_NODE_COUNT) {
            if (edgeMap.get(toVisit.peek()) != null) {
                Long crtNode = toVisit.poll();
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


                    if (edgeMap.get(edge.getDestination()) != null
                            && sourceCount <= MAX_DEGREE && dstCount <= MAX_DEGREE) {


                        usedEdges.add(edge);

                        countMap.put(edge.getSource(), sourceCount);
                        countMap.put(edge.getDestination(), dstCount);
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
                write(writer, usedEdges);
                toVisit.addAll(nodes);
//                }
                visited.add(crtNode);
                edgeMap.remove(crtNode);
                edges = null;
                nodes = null;
            } else {
                toVisit.poll();
                break;
            }
        }
        writer.flush();
        writer.close();
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


                    if (edgeMap.get(edge.getDestination()).size() >= MIN_DEGREE_TO_BFS && sourceCount <= MAX_DEGREE && dstCount <= MAX_DEGREE) {
                        usedEdges.add(edge);

                        countMap.put(edge.getSource(), sourceCount);
                        countMap.put(edge.getDestination(), dstCount);
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
                queue.poll();
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
