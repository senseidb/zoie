package proj.zoie.api;

public interface ZoieVersionFactory<V extends ZoieVersion>
{
  V getZoieVersion(String str);  
}
