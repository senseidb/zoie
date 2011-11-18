package proj.zoie.api;

public interface LifeCycleCotrolledDataConsumer<D> extends DataConsumer<D> {
  void start();
  void stop();
}
