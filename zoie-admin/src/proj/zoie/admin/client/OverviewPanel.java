package proj.zoie.admin.client;

import java.util.LinkedList;

import proj.zoie.admin.client.jmx.JMXAdminServiceAsync;
import proj.zoie.admin.client.jmx.RuntimeSystemInfo;
import proj.zoie.admin.client.jmx.ServerInfo;
import proj.zoie.admin.client.jmx.ZoieServerInfo;

import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.HTMLPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.visualization.client.AbstractDataTable;
import com.google.gwt.visualization.client.DataTable;
import com.google.gwt.visualization.client.VisualizationUtils;
import com.google.gwt.visualization.client.AbstractDataTable.ColumnType;
import com.google.gwt.visualization.client.visualizations.AreaChart;
import com.google.gwt.visualization.client.visualizations.PieChart;

public class OverviewPanel extends Composite {

	private static final int NUM_CPU_DISPLAY = 10;
	
	interface MyUiBinder extends UiBinder<Widget, OverviewPanel> {}
	private static MyUiBinder uiBinder = GWT.create(MyUiBinder.class);
	
	private JMXAdminServiceAsync _jmxService;
	
	@UiField HTMLPanel topPanel;
	@UiField FlowPanel memchartPanel;
	@UiField FlowPanel cpuchartPanel;
	@UiField Label serverLabel;
	@UiField Label platformLabel;
	@UiField Label availCPULabel;
	@UiField FlexTable infoTable;
	@UiField FlexTable dataProviderTable;
  private PieChart memChart;
	private AreaChart cpuChart;
	
	private Timer _refreshTimer;
	private LinkedList<long[]> _cpuTimeList;
	
	public OverviewPanel(JMXAdminServiceAsync jmxService){
		initWidget(uiBinder.createAndBindUi(this));
		_jmxService = jmxService;
		_cpuTimeList = new LinkedList<long[]>();
		updateServerInfo();
		Runnable onLoadCallback = new Runnable() {
	      public void run() {
	    	  memChart = new PieChart();
	    	  memchartPanel.add(memChart);
	    	  cpuChart = new AreaChart();
	    	  cpuchartPanel.add(cpuChart);
	    	  refreshSysInfo();
	      }
	    };
	    VisualizationUtils.loadVisualizationApi(onLoadCallback, PieChart.PACKAGE,AreaChart.PACKAGE);
	    _refreshTimer = new Timer() {
        public void run() {
        	refreshSysInfo();
          }
        };

        _refreshTimer.scheduleRepeating(3000);

	}
	
	private void updateServerInfo(){
		_jmxService.getServerInfo(new AsyncCallback<ServerInfo>() {

			public void onFailure(Throwable th) {
				updateLabels(null);
			}

			public void onSuccess(ServerInfo info) {
				updateLabels(info);
			}
			
			private void updateLabels(ServerInfo info){
				String serverString=null;
				String platformString=null;
				int availCPU = -1;
				if (info!=null){
					serverString = info.getServerVersion();
					platformString = info.getOsVersion();
					availCPU = info.getAvailCPU();
				}
				serverLabel.setText(serverString);
				platformLabel.setText(platformString);
				availCPULabel.setText(String.valueOf(availCPU));
			}
		});
	}

	private void refreshSysInfo(){
		 _jmxService.getRuntimeSystemInfo(new AsyncCallback<RuntimeSystemInfo>() {
				
				public void onSuccess(RuntimeSystemInfo usage) {
					try{
						_cpuTimeList.add(new long[]{usage.getCpuTime(),usage.getUserTime()});
						if (_cpuTimeList.size()>NUM_CPU_DISPLAY){
							_cpuTimeList.removeFirst();
						}
						
						AbstractDataTable memDataTable = createMemTable(usage);
						PieChart.Options memOptions = createMemOptions("Memory Usage");
						

						AbstractDataTable cpuDataTable = createCPUTable(_cpuTimeList);
						AreaChart.Options cpuOptions = createCPUOptions("CPU Time");
						
						memChart.draw(memDataTable,memOptions);
						cpuChart.draw(cpuDataTable,cpuOptions);
				//		pie.setVisible(true);
					}
					catch(Throwable th){
						memChart.draw(createMemTable(null),createMemOptions("Error: "+th.getMessage()));
					}
				}
				
				public void onFailure(Throwable throwable) {
					memChart.draw(createMemTable(null),createMemOptions("Error: "+throwable.getMessage()));
				}
			});
		 _jmxService.getZoieSystemInfo(new AsyncCallback<ZoieServerInfo>() {

      public void onFailure(Throwable arg0)
      {
        //TODO:
        infoTable.setText(0, 0, "ERROR");
      }

      public void onSuccess(ZoieServerInfo zsi)
      {
        String [] names = zsi.getNames();
        String [] values = zsi.getValues();
        for(int i = 0; i < names.length; i++)
        {
          infoTable.setText(i, 0, names[i]);
          infoTable.setText(i, 1, values[i]);
        }
      }
		 });
     _jmxService.getDataProviderInfo(new AsyncCallback<ZoieServerInfo>() {

       public void onFailure(Throwable arg0)
       {
         //TODO:
         dataProviderTable.setText(0, 0, "ERROR");
       }

       public void onSuccess(ZoieServerInfo zsi)
       {
         String [] names = zsi.getNames();
         String [] values = zsi.getValues();
         boolean [] writeable = zsi.getWritable();
         boolean [] readable = zsi.getReadable();
         int i = 0;
         for(i = 0; i < names.length; i++)
         {
           if (readable[i] && (!writeable[i]))
           {
             dataProviderTable.setText(i, 0, names[i]);
             dataProviderTable.setText(i, 1, values[i]);
           } else if (writeable[i])
           {
             dataProviderTable.setText(i, 0, names[i]);
             TextBox tb = new TextBox();
             tb.setText(values[i]);
             tb.addValueChangeHandler(new ValueChangeHandler<String>(){

              public void onValueChange(ValueChangeEvent<String> event)
              {
                System.out.println(event.getSource().toString());
              }});
             dataProviderTable.setWidget(i, 1, tb);
           }
         }
         Button buttonstart = new Button("start");
         buttonstart.addClickHandler(new ClickHandler(){

          public void onClick(ClickEvent arg0)
          {
            _jmxService.invokeNoParam("start", new AsyncCallback<Void>(){

              public void onFailure(Throwable arg0)
              {
                // TODO Auto-generated method stub
                
              }

              public void onSuccess(Void arg0)
              {
                // TODO Auto-generated method stub
                
              }});
          }});
         dataProviderTable.setWidget(i, 1, buttonstart);
         i++;
         Button buttonstop = new Button("stop");
         buttonstop.addClickHandler(new ClickHandler(){

           public void onClick(ClickEvent arg0)
           {
             _jmxService.invokeNoParam("stop", new AsyncCallback<Void>(){

               public void onFailure(Throwable arg0)
               {
                 // TODO Auto-generated method stub
                 
               }

               public void onSuccess(Void arg0)
               {
                 // TODO Auto-generated method stub
                 
               }});
           }});
         dataProviderTable.setWidget(i, 1, buttonstop);
       }
      });
	}
	
	static private AreaChart.Options createCPUOptions(String title) {
		AreaChart.Options options = AreaChart.Options.create();
	    options.setWidth(400);
	    options.setHeight(240);
	    options.setTitle(title);
	    return options;
	}
	
	static private PieChart.Options createMemOptions(String title) {
		PieChart.Options options = PieChart.Options.create();
	    options.setWidth(400);
	    options.setHeight(240);
	    options.set3D(true);
	    options.setTitle(title);
	    return options;
	}
	
	private static AbstractDataTable createCPUTable(LinkedList<long[]> cpuTimes) {
	    DataTable data = DataTable.create();
	    data.addColumn(ColumnType.NUMBER, "CPU Times (millis)");
	    data.addColumn(ColumnType.NUMBER, "User Times (millis)");
	    int size = cpuTimes.size();
	    data.addRows(size);
	    int idx = 0;
	    
	    for (long[] cpuTime : cpuTimes){
	    	long cpuInMillis = cpuTime[0]/1000000;
	    	long userInMillis = cpuTime[1]/1000000;
	    	data.setValue(idx, 0, cpuInMillis);
	    	data.setValue(idx, 1, userInMillis);
	    	idx++;
	    }
	    return data;
	}
	
	private static AbstractDataTable createMemTable(RuntimeSystemInfo sysInfo) {
	    DataTable data = DataTable.create();
	    data.addColumn(ColumnType.STRING, "Memory Usage");
	    data.addColumn(ColumnType.NUMBER, "Percentage");
	    data.addRows(2);
	    data.setValue(0, 0, "Free");
	    data.setValue(0, 1, sysInfo==null ? 0 : sysInfo.getFreeMemory());
	    data.setValue(1, 0, "Used");
	    data.setValue(1, 1, sysInfo==null ? 0 : sysInfo.getMaxMemory() - sysInfo.getFreeMemory());
	    return data;
	}
}
