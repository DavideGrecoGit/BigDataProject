package uk.ac.gla.dcs.bigdata.providedutilities;

import org.terrier.matching.models.WeightingModelLibrary;

/**
 * This utility class provides an implementation of the DPH scoring function
 * based on that provided as part of the Terrier.org IR platform.
 * 
 * It calculates the relevance score for a single <term,document> pair, given
 * some statistics of the corpus.
 * 
 * @author Richard
 *
 */
public class DPHScorer {
	
	/**
	 * Calculates the DPH score for a single query term in a document
	 * @param termFrequencyInCurrentDocument // The number of times the query appears in the document
	 * @param totalTermFrequencyInCorpus // the number of times the query appears in all documents
	 * @param currentDocumentLength // the length of the current document (number of terms in the document)
	 * @param averageDocumentLengthInCorpus // the average length across all documents
	 * @param totalDocsInCorpus // the number of documents in the corpus
	 * @return
	 */
	public static double getDPHScore(
			short termFrequencyInCurrentDocument,
			int totalTermFrequencyInCorpus,
			int currentDocumentLength,
			double averageDocumentLengthInCorpus,
			long totalDocsInCorpus) {
		
							
					// calculate the f and normalization components of DPH
					double f = WeightingModelLibrary.relativeFrequency(termFrequencyInCurrentDocument, currentDocumentLength);
					double norm = (1d-f) * (1d -f)/(termFrequencyInCurrentDocument+1d);
					 
					// calculate DPH score
					double DPHScore = norm 
							* (termFrequencyInCurrentDocument
							* WeightingModelLibrary.log((termFrequencyInCurrentDocument * averageDocumentLengthInCorpus/currentDocumentLength) * (totalDocsInCorpus/totalTermFrequencyInCorpus) )
					 	    + 0.5d 
					 	    * WeightingModelLibrary.log(2d*Math.PI*termFrequencyInCurrentDocument*(1d-f))
					);
					
					return DPHScore;
		
	}
	
}
