package my.app.microbucketj.client.model;

public record LeaderHint(String leaderId, String host, int port) {
  public boolean present() {
    return host != null && !host.isBlank() && port > 0;
  }
}
