package my.app.microbucketj.client.exception;

import my.app.microbucketj.client.model.ReportResult;

public final class AdaptiveRateReportException extends AdaptiveRateException {
  private final boolean successReport;
  private final ReportResult reportResult;

  public AdaptiveRateReportException(String bucket, boolean successReport, ReportResult reportResult) {
    super(bucket, "Adaptive rate report failed for bucket=" + bucket + ", successReport=" + successReport
        + ", statusCode=" + (reportResult != null ? reportResult.statusCode() : -1));
    this.successReport = successReport;
    this.reportResult = reportResult;
  }

  public boolean successReport() {
    return successReport;
  }

  public ReportResult reportResult() {
    return reportResult;
  }
}
