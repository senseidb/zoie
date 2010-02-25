package proj.zoie.tools.luke;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.getopt.luke.LukePlugin;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieIndexReader;

public class ZoiePlugin extends LukePlugin {
	private ZoieIndexReader _zoieReader;
	
	public ZoiePlugin() {
		_zoieReader = null;
	}

	@Override
	public String getPluginHome() {
		return "Zoie Luke Plugin";
	}

	@Override
	public String getPluginInfo() {
		return "mailto:john.wang@gmail.com";
	}

	@Override
	public String getPluginName() {
		return "Zoie Luke Plugin";
	}

	@Override
	public String getXULName() {
		return "/zoie-luke.xml";
	}

	@Override
	public boolean init() throws Exception {
		return true;
	}
	
	public void toUID(){
		Object srcArea = app.find(myUi, "docids");
		String val = app.getString(srcArea, "text");
		if (val!=null && val.length() > 0){
			try{
				String[] docids = val.split(",");
				IntList idList = new IntArrayList(docids.length);
				for (String docid : docids){
					idList.add(Integer.parseInt(docid.trim()));
				}
				int[] idArray = idList.toIntArray();
				StringBuffer sbuf = new StringBuffer();
				ZoieIndexReader zoieReader = getZoieReader();
				boolean first = true;
				for (int docid : idArray){
					long uid;
					try{
					  uid = zoieReader.getUID(docid);
					}
					catch(Exception e){
					  uid=-1;
					}
					if (!first){
					  sbuf.append(", ");
				    }
				    else{
					  first = false;
				    }
				    sbuf.append(uid);
				}
				Object targetArea = app.find(myUi,"uids");
				app.setString(targetArea, "text", sbuf.toString());
			}
			catch(Exception e){
				e.printStackTrace();
				app.showStatus("invalid input string: "+val);
			}
		}
	}
	
	public void toDocID(){
		Object srcArea = app.find(myUi, "uids");
		String val = app.getString(srcArea, "text");
		if (val!=null && val.length() > 0){
			try{
				String[] uids = val.split(",");
				IntList idList = new IntArrayList(uids.length);
				for (String uid : uids){
					idList.add(Integer.parseInt(uid.trim()));
				}
				int[] idArray = idList.toIntArray();
				StringBuffer sbuf = new StringBuffer();
				ZoieIndexReader zoieReader = getZoieReader();
				DocIDMapper mapper = zoieReader.getDocIDMaper();
				boolean first = true;
				for (int uid : idArray){
					int docid;
					try{
					  docid = mapper.getDocID(uid);
					}
					catch(Exception e){
					  docid=-1;
					}
					if (!first){
						sbuf.append(", ");
					}
					else{
						first = false;
					}
					sbuf.append(docid);
				}
				Object targetArea = app.find(myUi,"docids");
				app.setString(targetArea, "text", sbuf.toString());
			}
			catch(Exception e){

				e.printStackTrace();
				app.showStatus("invalid input string: "+val);
			}
		}
	}
	
	private ZoieIndexReader getZoieReader() throws IOException{
		if (_zoieReader == null){
			IndexReader innerReader = super.getIndexReader();
			_zoieReader = ZoieIndexReader.open(innerReader);
		}
		return _zoieReader;
	}
}
