package uk.ac.gla.dcs.bigdata.providedstructures;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a single news article from the source Washington Post Corpus
 * @author Richard
 *
 */
public class NewsArticle implements Serializable {

	private static final long serialVersionUID = 7860293794078412243L;
	
	String id; // unique article identifier
	String article_url; // url pointing to the online article
	String title; // article title
	String author; // article author
	long published_date; // publication date as a unix timestamp (ms)
	List<ContentItem> contents; // the contents of the article body
	String type; // type of the article
	String source; // news provider
	
	public NewsArticle() {}
	
	public NewsArticle(String id, String article_url, String title, String author, long published_date,
			List<ContentItem> contents, String type, String source) {
		super();
		this.id = id;
		this.article_url = article_url;
		this.title = title;
		this.author = author;
		this.published_date = published_date;
		this.contents = contents;
		this.type = type;
		this.source = source;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getArticle_url() {
		return article_url;
	}

	public void setArticle_url(String article_url) {
		this.article_url = article_url;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public long getPublished_date() {
		return published_date;
	}

	public void setPublished_date(long published_date) {
		this.published_date = published_date;
	}

	public List<ContentItem> getContents() {
		return contents;
	}

	public void setContents(List<ContentItem> contents) {
		this.contents = contents;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}
	
	
}
