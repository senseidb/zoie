package proj.zoie.api;

public class ZoieHealth
{
  public static final long HEALTH_OK = 0;
  public static final long HEALTH_FATAL = 100;
  public static volatile long health = HEALTH_OK;
  public static void setFatal()
  {
    health = HEALTH_FATAL;
  }
  public static void setOK()
  {
    health = HEALTH_OK;
  }
  public static long getHealth()
  {
    return health;
  }
}
