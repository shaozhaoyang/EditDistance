package eu.unitn.disi.db.query;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.FileOperator;

public class WildCardQuery {
	private ArrayList<String> query;
	private LinkedList<String> wildCardQuery;
	private int threshold;
	private String queryDir;
	private int total;
	private int[] chosenNum;
	private int count;
	private String dirName;
	private String fileNameWithoutSuffix;
	private Set<Multigraph> wcQueries;
	public WildCardQuery(){
	}
	
	public WildCardQuery(int threshold){
		this.wcQueries = new HashSet<>();
		this.wildCardQuery = new LinkedList<String>();
		this.threshold = threshold;
		count = 0;
	}
	
	public void run(String fileName) throws IOException{
		this.readQuery(fileName);
		this.choose(threshold, 0);
	}
	
	public void readQuery(String fileName) throws IOException{
		File queryFile = new File(fileName);
		fileNameWithoutSuffix = this.getFileNameWithoutSuffix(fileName);
		queryDir = queryFile.getParent();
		query = FileOperator.readQuery(queryFile);
		total = query.size();
		chosenNum = new int[threshold];
	}
	
	public String getFileNameWithoutSuffix(String fileName){
		String temp = fileName.split("\\.")[0];
		String[] words = temp.split("/");
		return words[words.length-1];
	}
	public void createWildCardQueryDir(){
		dirName = queryDir + "/wildcard_" +fileNameWithoutSuffix + "_" + threshold + "/";
		FileOperator.createDir(dirName);
		
	}
	
	public Edge changeToWildCard(String edge){
		String[] words = edge.split(" ");
		return new Edge(Long.parseLong(words[0]), Long.parseLong(words[1]), 0L);
	}
	
	public Edge stringToEdge(String edge){
		String[] words = edge.split(" ");
		return new Edge(Long.parseLong(words[0]), Long.parseLong(words[1]), Long.parseLong(words[2]));
	}
	
	public void writeQuery() throws IOException{
//		BufferedWriter bw = new BufferedWriter(new FileWriter(dirName+"query" + count + ".txt"));
		Multigraph newQuery = new BaseMultigraph();
		boolean isChanging = false;
		for(int i = 0; i < query.size(); i++){
			for(int j = 0; j < chosenNum.length; j ++){
				if(i == chosenNum[j]){
					isChanging = true;
					break;
				}
			}
			if(isChanging){
				isChanging = false;
				Edge newEdge = this.changeToWildCard(query.get(i));
				newQuery.addVertex(newEdge.getSource());
				newQuery.addVertex(newEdge.getDestination());
				newQuery.addEdge(newEdge);
//				bw.write(changeToWildCard(query.get(i)));
//				bw.newLine();
			}else{
				Edge newEdge = this.stringToEdge(query.get(i));
				newQuery.addVertex(newEdge.getSource());
				newQuery.addVertex(newEdge.getDestination());
				newQuery.addEdge(newEdge);
//				bw.write(query.get(i));
//				bw.newLine();
			}
		}
		this.wcQueries.add(newQuery);
//		bw.close();
	}
	
	public void choose(int number, int index) throws IOException{
		if(number == 0){
			this.writeQuery();
			count ++;
			return;
		}
		for(int i = index; i < total; i++){
			chosenNum[number - 1] = i;
			choose(number-1,i+1);
		}
	}

	public ArrayList<String> getQuery() {
		return query;
	}

	public void setQuery(ArrayList<String> query) {
		this.query = query;
	}

	public LinkedList<String> getWildCardQuery() {
		return wildCardQuery;
	}

	public void setWildCardQuery(LinkedList<String> wildCardQuery) {
		this.wildCardQuery = wildCardQuery;
	}

	public int getThreshold() {
		return threshold;
	}

	public void setThreshold(int threshold) {
		this.threshold = threshold;
	}

	public String getQueryDir() {
		return queryDir;
	}

	public void setQueryDir(String queryDir) {
		this.queryDir = queryDir;
	}

	public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	public int[] getChosenNum() {
		return chosenNum;
	}

	public void setChosenNum(int[] chosenNum) {
		this.chosenNum = chosenNum;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getDirName() {
		return dirName;
	}

	public void setDirName(String dirName) {
		this.dirName = dirName;
	}

	public String getFileNameWithoutSuffix() {
		return fileNameWithoutSuffix;
	}

	public void setFileNameWithoutSuffix(String fileNameWithoutSuffix) {
		this.fileNameWithoutSuffix = fileNameWithoutSuffix;
	}

	public Set<Multigraph> getWcQueries() {
		return wcQueries;
	}

	public void setWcQueries(Set<Multigraph> wcQueries) {
		this.wcQueries = wcQueries;
	}
}
