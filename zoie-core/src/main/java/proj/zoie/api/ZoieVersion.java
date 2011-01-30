package proj.zoie.api;


public abstract class ZoieVersion implements Comparable<ZoieVersion>
{
  public abstract String encodeToString();
  public static <T extends ZoieVersion> T max(T a, T b)
  {
    return a == null ? b : ((a.compareTo(b) < 0) ? b : a);
  }
}