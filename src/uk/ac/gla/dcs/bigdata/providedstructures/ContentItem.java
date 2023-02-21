package uk.ac.gla.dcs.bigdata.providedstructures;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * News articles are divided into multiple types of content block. A ContentItem represents one of these pieces of content.
 * 
 * @author Richard
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true) // this tells Jackson to ignore any properties found in the json that does not have an associated class variable
public class ContentItem implements Serializable {

	private static final long serialVersionUID = -664641145229312192L;

	String content; // the string content of the item
	String subtype; // an optional subtype for the item (e.g. paragraph)
	String type; // the type of the content (usually sanitized_html)
	String mime; // mime (Multipurpose Internet Mail Extensions) type of the item (usually text/html)

	// if image
	String fullcaption; // caption information
	String imageURL; // url of image
	String imageHeight; // image height
	String imageWidth; // image width
	String blurb; // image associated text

	// if creditation
	String role;
	String name;
	String bio;
	
	// if Kicker
	String kicker;

	public ContentItem() {}


	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getSubtype() {
		return subtype;
	}

	public void setSubtype(String subtype) {
		this.subtype = subtype;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getMime() {
		return mime;
	}

	public void setMime(String mime) {
		this.mime = mime;
	}

	public String getFullcaption() {
		return fullcaption;
	}

	public void setFullcaption(String fullcaption) {
		this.fullcaption = fullcaption;
	}

	public String getImageURL() {
		return imageURL;
	}

	public void setImageURL(String imageURL) {
		this.imageURL = imageURL;
	}


	public String getImageHeight() {
		return imageHeight;
	}


	public void setImageHeight(String imageHeight) {
		this.imageHeight = imageHeight;
	}


	public String getImageWidth() {
		return imageWidth;
	}


	public void setImageWidth(String imageWidth) {
		this.imageWidth = imageWidth;
	}


	public String getBlurb() {
		return blurb;
	}


	public void setBlurb(String blurb) {
		this.blurb = blurb;
	}


	public String getRole() {
		return role;
	}


	public void setRole(String role) {
		this.role = role;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public String getBio() {
		return bio;
	}


	public void setBio(String bio) {
		this.bio = bio;
	}


	public String getKicker() {
		return kicker;
	}


	public void setKicker(String kicker) {
		this.kicker = kicker;
	}


}
