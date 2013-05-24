package proj.zoie.perf.client;

import java.util.Comparator;

public class ZoiePerfVersion implements Comparable<ZoiePerfVersion> {

  public final long countVersion;
  public final long offsetVersion;

  ZoiePerfVersion(long v1, long v2) {
    countVersion = v1;
    offsetVersion = v2;
  }

  public static String toString(ZoiePerfVersion version) {
    return toString(version.countVersion, version.offsetVersion);
  }

  public static String toString(long count, long offset) {
    StringBuilder buf = new StringBuilder();
    buf.append(count).append(":").append(offset);
    return buf.toString();
  }

  public static ZoiePerfVersion fromString(String version) {
    long v1 = 0, v2 = 0;

    if (version != null && version.length() > 0) {
      String[] parts = version.split(":");
      v1 = Long.parseLong(parts[0]);
      v2 = Long.parseLong(parts[1]);
    }
    return new ZoiePerfVersion(v1, v2);
  }

  public static final Comparator<String> COMPARATOR = new ZoiePerfVersionComparator();

  private static class ZoiePerfVersionComparator implements Comparator<String> {
    @Override
    public int compare(String o1, String o2) {
      ZoiePerfVersion v1 = ZoiePerfVersion.fromString(o1);
      ZoiePerfVersion v2 = ZoiePerfVersion.fromString(o2);
      return v1.compareTo(v2);
    }
  }

  @Override
  public int compareTo(ZoiePerfVersion o) {
    if (offsetVersion == o.offsetVersion) return 0;
    if (offsetVersion < o.offsetVersion) return -1;
    return 1;
  }
}
