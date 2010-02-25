package proj.zoie.admin.client;

import proj.zoie.admin.client.jmx.JMXAdminService;
import proj.zoie.admin.client.jmx.JMXAdminServiceAsync;
import proj.zoie.admin.client.search.SearchService;
import proj.zoie.admin.client.search.SearchServiceAsync;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.TabPanel;
import com.google.gwt.user.client.ui.TextBox;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class ZoieAdmin implements EntryPoint {
  /**
   * The message displayed to the user when the server cannot be reached or
   * returns an error.
   */
  private static final String SERVER_ERROR = "An error occurred while "
      + "attempting to contact the server. Please check your network "
      + "connection and try again.";

  private final SearchServiceAsync searchService = GWT.create(SearchService.class);
  private final JMXAdminServiceAsync jmxService = GWT.create(JMXAdminService.class);
  
  /**
   * This is the entry point method.
   */
  public void onModuleLoad() {
    final Button sendButton = new Button("Send");
    final TextBox nameField = new TextBox();
    nameField.setText("GWT User");

    // We can add style names to widgets
    sendButton.addStyleName("sendButton");
    
    TabPanel topTab = new TabPanel();
    topTab.setWidth("100%");
    topTab.setHeight("100%");
    topTab.setAnimationEnabled(true);
    
    topTab.add(new OverviewPanel(jmxService), "Overview");
    topTab.add(new SearchPanel(searchService),"Search");
    
    topTab.selectTab(0);
    RootPanel.get("toptab").add(topTab);
  }
}
