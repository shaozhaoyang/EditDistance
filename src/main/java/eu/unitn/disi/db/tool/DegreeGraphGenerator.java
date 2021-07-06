package eu.unitn.disi.db.tool;

import eu.unitn.disi.db.grava.graphs.Edge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */

/**
 * @author Zhaoyang
 *
 */
public class DegreeGraphGenerator {

    private static final long MAX_NODE_COUNT = 10000;
    private static final int MAX_DEGREE = 5;
    private static final int MIN_DEGREE_TO_BFS = 5;
    private long nodeCount = 0;

    public DegreeGraphGenerator(String graphFileName, String subGraphName) throws IOException {
        final Map<Long, Set<Edge>> graphEdges = read(graphFileName);
        final Map<Long, Set<Edge>> subGraphEdges = read(subGraphName);
        int crtDegree = 5;
        for (int i = 5; i <= 5; i += 5) {
            bfs(graphEdges, subGraphEdges, i + crtDegree, i);
        }
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
//		String a = "<http://rdf.freebase.com/ns/award.award_winner>";
//		System.out.println(a.split("/")[a.split("/").length-1]);
        DegreeGraphGenerator rf = new DegreeGraphGenerator("graph/1000000nodes-sout.graph", "graph/10000nodes-d10-sout.graph");
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

    private void bfs(final Map<Long, Set<Edge>> graphEdgeMap, final Map<Long, Set<Edge>> subGraphEdgeMap,
                     final int maxDegree, final int degreeIncrease) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(+MAX_NODE_COUNT + "nodes-sout" + "-d" + maxDegree +
            ".graph",
            false));

        int crtDegree = 0;
        for (Map.Entry<Long, Set<Edge>> subEntry : subGraphEdgeMap.entrySet()) {
            Set<Edge> toAdd = graphEdgeMap.getOrDefault(subEntry.getKey(), new HashSet<>());
            Set<Edge> subGraphEdge = subEntry.getValue();

            List<Edge> newEdges = new LinkedList<>(subEntry.getValue());
            int crt = degreeIncrease;
            for (Edge edge : toAdd) {
                if (!subGraphEdge.contains(edge)) {
                    crt--;
                    newEdges.add(edge);

                    if (crt <= 0) {
                        break;
                    }
                }
            }
            crtDegree += newEdges.size();
            write(writer, newEdges);
        }


//        System.out.println(allNodes.size());
        System.out.println("avg degree:" + crtDegree / (subGraphEdgeMap.keySet().size()));
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
