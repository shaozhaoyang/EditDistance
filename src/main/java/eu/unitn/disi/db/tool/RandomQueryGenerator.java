package eu.unitn.disi.db.tool;

import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class RandomQueryGenerator {

    private static final Random RANDOM = new Random();


    private static void writeToFile(Set<String> query, String node, int maxFreq, int crtFreq) {
        try {
            String fileName =
                    "queryFolder/10000nodes-d20" + "/E8" + node + "_" + crtFreq + ".txt";
            final BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            for (String str : query) {
                writer.write(str);
                writer.newLine();
            }
            writer.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    private static String edgeOutput(Edge edge) {
        return edge.getSource() + " " + edge.getDestination() + " " + edge.getLabel();
    }

    private static Edge getRandomEdge(Collection<Edge> edges) {
        List<Edge> list = new ArrayList<>(edges);
        return list.get(RANDOM.nextInt(list.size()));
    }


    public void generateQuery(Multigraph G, List<Long> nodes) {
        int maxEdgeNum = 8;
        Set<String> query = new HashSet<>();
        int currentFreq = 0;
        int maxFreq = 5000;

        Long startingNode = null;

        while (true) {
            startingNode = nodes.get(RANDOM.nextInt(nodes.size()));
            if (G.outgoingEdgesOf(startingNode).size() > 1 && G.incomingEdgesOf(startingNode).size() > 1) {
                break;
            }
        }
        int edgeNum = 0;
        Set<Long> visited = new HashSet<>();
        Set<Edge> visitedEdges = new HashSet<>();
        Long currentNode;
        LinkedList<Long> toVisit = new LinkedList<>();
        toVisit.addFirst(startingNode);
        Random random = new Random();
        while (edgeNum < maxEdgeNum) {
            Collections.shuffle(toVisit);
            currentNode = toVisit.peek();
            List<Edge> edges = new ArrayList<>();
            for (Edge edge : G.incomingEdgesOf(currentNode)) {
                if (!visitedEdges.contains(edge)) {
                    edges.add(edge);
                }
            }
            for (Edge edge : G.outgoingEdgesOf(currentNode)) {
                if (!visitedEdges.contains(edge)) {
                    edges.add(edge);
                }
            }
            if (edges.isEmpty()) {
                visited.add(currentNode);
                toVisit.remove(currentNode);
            } else {
                int index = random.nextInt(edges.size());
                query.add(edgeOutput(edges.get(index)));
                currentFreq += G.getLabelFreq().get(edges.get(index).getLabel()).getFrequency();
                visitedEdges.add(edges.get(index));
                if (currentNode.equals(edges.get(index).getSource())) {
                    if (!visited.contains(edges.get(index).getDestination())) {
                        toVisit.add(edges.get(index).getDestination());
                    }
                } else {
                    if (!visited.contains(edges.get(index).getSource())) {
                        toVisit.add(edges.get(index).getSource());
                    }
                }
                edgeNum++;
            }
        }
        if (currentFreq < maxFreq) {
            writeToFile(query, String.valueOf(startingNode), maxFreq, currentFreq);
        }
    }
}
