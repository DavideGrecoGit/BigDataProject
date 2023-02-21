package uk.ac.gla.dcs.bigdata.providedstructures;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.List;

public class DocumentRanking implements Serializable{

	private static final long serialVersionUID = 2675653150567946299L;
	
	Query query;
	List<RankedResult> results;
	
	public DocumentRanking() {}
	
	public DocumentRanking(Query query, List<RankedResult> results) {
		super();
		this.query = query;
		this.results = results;
	}
	
	public Query getQuery() {
		return query;
	}
	public void setQuery(Query query) {
		this.query = query;
	}
	public List<RankedResult> getResults() {
		return results;
	}
	public void setResults(List<RankedResult> results) {
		this.results = results;
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(query.getOriginalQuery());
		builder.append("\n");
		
		int counter = 1;
		for (RankedResult result : results) {
			builder.append("  ");
			builder.append(counter);
			builder.append(":");
			builder.append(result.getDocid());
			builder.append(" ");
			builder.append(result.getScore());
			builder.append(" ");
			builder.append(result.getArticle().getTitle());
			builder.append("\n");
			counter++;
		}
		
		
		return builder.toString();
	}
	
	public void write(String outDirectory) {
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outDirectory+"/"+query.getOriginalQuery().replace(" ", "_"))));
			writer.write(this.toString());
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
