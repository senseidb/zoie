package proj.zoie.api.impl;
//import java.util.ArrayList;


import proj.zoie.api.ZoieVersion;
import proj.zoie.api.ZoieVersionFactory;
import proj.zoie.api.DefaultZoieVersion;

public class DefaultZoieVersionFactory implements ZoieVersionFactory<ZoieVersion>
{ 

  public DefaultZoieVersionFactory()
  {  
  }

  public DefaultZoieVersion getZoieVersion(String str)
  {
     if(str.equals("null"))
     {   
       return null;
     }
 
     DefaultZoieVersion zvt = new DefaultZoieVersion();
     long id = Long.parseLong(str);
     zvt.setVersionId(id);
 
     return zvt;  
   }
}