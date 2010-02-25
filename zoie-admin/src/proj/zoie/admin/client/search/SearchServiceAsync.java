package proj.zoie.admin.client.search;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface SearchServiceAsync {
	void search(SearchRequest req, AsyncCallback<SearchResult> callback);
}
