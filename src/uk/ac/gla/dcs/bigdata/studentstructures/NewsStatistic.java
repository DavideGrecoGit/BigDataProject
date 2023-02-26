package src.uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.HashMap;

public class NewsStatistic implements Serializable {

	private static final long serialVersionUID = -6564778378850787248L;
	
	private HashMap<String, Integer> termFrequencyMap;
	private int docLength;
	
	public NewsStatistic() {}
	
	public NewsStatistic(HashMap<String, Integer> termFrequencyMap, int docLength) {
		this.termFrequencyMap = termFrequencyMap;
		this.docLength = docLength;
	}

	public HashMap<String, Integer> getTermFrequencyMap() {
		return termFrequencyMap;
	}

	public void setTermFrequencyMap(HashMap<String, Integer> termFrequencyMap) {
		this.termFrequencyMap = termFrequencyMap;
	}
	
	public int getDocLength() {
		return docLength;
	}

	public void setDocLength(int docLength) {
		this.docLength = docLength;
	}

	public void incrementDocLenght(){
		this.docLength = this.docLength + 1;
	}

	public Boolean isEmpty(){
		if(this.docLength == 0){
			return true;
		}
		return false;
	}

}
