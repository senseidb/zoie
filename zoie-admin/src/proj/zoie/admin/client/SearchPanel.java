package proj.zoie.admin.client;

import java.util.Map;

import proj.zoie.admin.client.search.SearchHit;
import proj.zoie.admin.client.search.SearchRequest;
import proj.zoie.admin.client.search.SearchResult;
import proj.zoie.admin.client.search.SearchServiceAsync;

import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Document;
import com.google.gwt.dom.client.Element;
import com.google.gwt.dom.client.SpanElement;
import com.google.gwt.dom.client.TableElement;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.KeyCodes;
import com.google.gwt.event.dom.client.KeyUpEvent;
import com.google.gwt.event.dom.client.KeyUpHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

public class SearchPanel extends Composite {
  interface MyUiBinder extends UiBinder<Widget, SearchPanel> {}
  private static MyUiBinder uiBinder = GWT.create(MyUiBinder.class);
  private final SearchServiceAsync _searchSvc;

  private Document doc = Document.get();
	  
  @UiField Button searchButton;
  @UiField TextBox queryInput;
  @UiField SpanElement hitcountLabel;
  @UiField SpanElement searchtimeLabel;
  @UiField TableElement resultTable;
  
  public SearchPanel(SearchServiceAsync searchSvc){
	  _searchSvc = searchSvc;
	  initWidget(uiBinder.createAndBindUi(this));
	  
	  class MyHandler implements ClickHandler, KeyUpHandler {

	        public void onClick(ClickEvent event) {
	          doSearch();
	        }

	        public void onKeyUp(KeyUpEvent event) {
	          if (event.getNativeKeyCode() == KeyCodes.KEY_ENTER) {
	        	  doSearch();
	          }
	        }

	        private void doSearch() {
	          String query = queryInput.getValue(); 
	          SearchRequest req = new SearchRequest();
	          req.setQuery(query);
	          _searchSvc.search(req, new AsyncCallback<SearchResult>(){

	  			public void onFailure(Throwable error) {
	  				hitcountLabel.setInnerHTML("<b>0/0</b>");
	  				searchtimeLabel.setInnerHTML("<b>0</b>");
	  			}
	  			
	  			private void removeAllResults(Element elem){
	  				while (elem.getChildCount()>0){
	  					elem.removeChild(elem.getFirstChild());
	  				}
	  			}
	  			
	  			private void renderDoc(SearchHit doc,Element elem){
	  				Map<String,String[]> fields = doc.getFields();
	  				String path = fields.get("path")[0];
	  				String frag = fields.get("fragment")[0];
	  				String name;
	  				int idx = path.lastIndexOf("/");
	  				if (idx<0){
	  					name = path;
	  				}
	  				else{
	  					name = path.substring(idx+1);
	  				}
	  				
	  				String nameStr="<a class=\"hitlink\" href=\"file:///"+path+"\">"+name+"</a>";
	  				String fragStr="<div class=\"frag\">"+frag+"</div>";
	  				String pathStr="<span class=\"path\">"+path+"</span>";
	  				String scoreStr="<span class=\"score\">score: "+doc.getScore()+"</span>";
	  				
	  				elem.setInnerHTML(nameStr+fragStr+"<div>"+pathStr+" - "+scoreStr+"</div>");
	  			}

	  			public void onSuccess(SearchResult res) {
	  				StringBuilder buf = new StringBuilder();
	  				buf.append("<b>").append(res.getTotalHits()).append("/").append(res.getTotalDocs()).append("</b>");
	  				hitcountLabel.setInnerHTML(buf.toString());
	  				StringBuilder buf2 = new StringBuilder();
	  				buf2.append("<b>").append(String.valueOf(res.getTime())).append("</b>");
	  				searchtimeLabel.setInnerHTML(buf2.toString());
	  				
	  				// populate table elements
	  				removeAllResults(resultTable);
	  				SearchHit[] hits = res.getHits();
	  				for (SearchHit hit : hits){

	  					Element row = doc.createElement("tr");
	  					resultTable.appendChild(row);
	  					Element col = doc.createElement("td");
	  					row.appendChild(col);
	  					renderDoc(hit,col);
	  				}
	  			}
	          	
	          });
	        }
	      }
	 

      hitcountLabel.setInnerHTML("<b>0/0</b>");
	  searchtimeLabel.setInnerHTML("<b>0</b>");
	  
	// Add a handler to send the name to the server
      MyHandler handler = new MyHandler();
      searchButton.addClickHandler(handler);
      queryInput.addKeyUpHandler(handler);
      queryInput.setFocus(true);
  }
}
