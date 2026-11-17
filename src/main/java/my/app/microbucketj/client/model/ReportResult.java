package my.app.microbucketj.client.model;

import java.io.Serializable;

import my.app.microbucketj.client.protocol.ApiProtocol;

public record ReportResult(byte statusCode) implements Serializable {
  public boolean ok() {
    return statusCode == ApiProtocol.ST_OK;
  }

  public boolean unavailable() {
    return statusCode == ApiProtocol.ST_UNAVAILABLE;
  }

  public boolean notFound() {
    return statusCode == ApiProtocol.ST_NOT_FOUND;
  }

  public boolean collision() {
    return statusCode == ApiProtocol.ST_COLLISION;
  }

  public static ReportResult unavailableResult() {
    return new ReportResult(ApiProtocol.ST_UNAVAILABLE);
  }
}
