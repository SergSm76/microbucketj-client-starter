package my.app.microbucketj.client.netty;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import my.app.microbucketj.client.MicroAdaptiveRateClient;
import my.app.microbucketj.client.exception.AdaptiveRateCallException;
import my.app.microbucketj.client.exception.AdaptiveRateDeniedException;
import my.app.microbucketj.client.exception.AdaptiveRateNotFoundException;
import my.app.microbucketj.client.exception.AdaptiveRateReportException;
import my.app.microbucketj.client.exception.AdaptiveRateUnavailableException;
import my.app.microbucketj.client.model.AcquireResult;
import my.app.microbucketj.client.model.AcquireStatus;
import my.app.microbucketj.client.model.ReportResult;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public final class NettyMicroAdaptiveRateClient implements MicroAdaptiveRateClient {

  private final NettyMicroBucketJClient core;

  public NettyMicroAdaptiveRateClient(NettyMicroBucketJClient core) {
    this.core = Objects.requireNonNull(core, "core");
  }

  @Override
  public boolean tryAcquire(String bucket) {
    return core.tryAcquire(bucket, 1);
  }

  @Override
  public boolean tryAcquire(String bucket, long permits) {
    return core.tryAcquire(bucket, permits);
  }

  @Override
  public AcquireResult tryAcquireDetailed(String bucket) {
    return core.tryAcquireDetailed(bucket, 1);
  }

  @Override
  public AcquireResult tryAcquireDetailed(String bucket, long permits) {
    return core.tryAcquireDetailed(bucket, permits);
  }

  @Override
  public CompletableFuture<Boolean> tryAcquireAsync(String bucket) {
    return core.tryAcquireAsync(bucket, 1);
  }

  @Override
  public CompletableFuture<AcquireResult> tryAcquireDetailedAsync(String bucket) {
    return core.tryAcquireDetailedAsync(bucket, 1);
  }

  @Override
  public ReportResult report(String bucket, boolean success) {
    return core.report(bucket, success);
  }

  @Override
  public CompletableFuture<ReportResult> reportAsync(String bucket, boolean success) {
    return core.reportAsync(bucket, success);
  }

  @Override
  @SuppressFBWarnings("THROWS_METHOD_THROWS_RUNTIMEEXCEPTION")
  public <T> T executeWithAdaptiveRate(String bucket, Supplier<T> action) {
    Objects.requireNonNull(action, "action");
    AcquireResult acquire = tryAcquireDetailed(bucket);
    ensureAllowed(bucket, acquire);

    try {
      T result = action.get();
      ensureReportOk(bucket, report(bucket, true));
      return result;
    } catch (RuntimeException ex) {
      attachSuppressedReportFailure(ex, bucket, report(bucket, false));
      throw ex;
    } catch (Exception ex) {
      AdaptiveRateCallException wrapped = new AdaptiveRateCallException(bucket, ex);
      attachSuppressedReportFailure(wrapped, bucket, report(bucket, false));
      throw wrapped;
    }
  }

  @Override
  public void executeWithAdaptiveRate(String bucket, Runnable action) {
    Objects.requireNonNull(action, "action");
    executeWithAdaptiveRate(bucket, () -> {
      action.run();
      return null;
    });
  }

  @Override
  public <T> CompletableFuture<T> executeWithAdaptiveRateAsync(
      String bucket,
      Supplier<CompletableFuture<T>> action
  ) {
    return tryAcquireDetailedAsync(bucket)
        .thenCompose(acquire -> {
          ensureAllowed(bucket, acquire);
          CompletableFuture<T> actionFuture;
          try {
            actionFuture = action.get();
          } catch (RuntimeException ex) {
            return reportAsync(bucket, false)
                .handle((report, repEx) -> {
                  updateExceptionAndResult(bucket, ex, report, repEx);
                  return null;
                });
          } catch (Throwable ex) {
            AdaptiveRateCallException wrapped = new AdaptiveRateCallException(bucket, ex);
            return reportAsync(bucket, false)
                .handle((report, repEx) -> {
                  updateExceptionAndResult(bucket, wrapped, report, repEx);
                  return null;
                });
          }

          return actionFuture.thenCompose(result ->
              reportAsync(bucket, true).thenApply(report -> {
                ensureReportOk(bucket, report);
                return result;
              })
          ).handle((result, error) -> {
            Throwable unwrapped = unwrap(error);
            if (unwrapped == null) {
              return CompletableFuture.completedFuture(result);
            }
            Throwable failure = normalizeActionFailure(bucket, unwrapped);
            return reportAsync(bucket, false).handle((report, repEx) -> {
              attachSuppressedReportFailure(failure, bucket, report, repEx);
              sneakyThrow(failure);
              return (T) null;
            });
          }).thenCompose(future -> future);
        });
  }

  @SuppressFBWarnings("THROWS_METHOD_THROWS_RUNTIMEEXCEPTION")
  private void updateExceptionAndResult(String bucket, RuntimeException ex, ReportResult report, Throwable repEx) throws RuntimeException {
    attachSuppressedReportFailure(ex, bucket, report, repEx);
    throw ex;
  }

  private static void ensureAllowed(String bucket, AcquireResult acquire) {
    AcquireStatus status = acquire.status();
    if (status == AcquireStatus.ALLOWED) {
      return;
    }
    switch (status) {
      case NOT_FOUND, COLLISION -> throw new AdaptiveRateNotFoundException(bucket, acquire);
      case DENIED, SHEDDED, BREAKER_OPEN -> throw new AdaptiveRateDeniedException(bucket, acquire);
      default -> throw new AdaptiveRateUnavailableException(bucket, acquire);
    }
  }

  private static void ensureReportOk(String bucket, ReportResult reportResult) {
    if (reportResult != null && reportResult.ok()) {
      return;
    }
    throw new AdaptiveRateReportException(bucket, true,
        reportResult != null ? reportResult : ReportResult.unavailableResult());
  }

  private static Throwable normalizeActionFailure(String bucket, Throwable error) {
    Throwable unwrapped = unwrap(error);
    if (unwrapped instanceof RuntimeException || unwrapped instanceof Error) {
      return unwrapped;
    }
    return new AdaptiveRateCallException(bucket, unwrapped);
  }

  private static void attachSuppressedReportFailure(Throwable original, String bucket, ReportResult reportResult) {
    if (reportResult != null && reportResult.ok()) {
      return;
    }
    original.addSuppressed(new AdaptiveRateReportException(bucket, false,
        reportResult != null ? reportResult : ReportResult.unavailableResult()));
  }

  private static void attachSuppressedReportFailure(Throwable original, String bucket, ReportResult reportResult, Throwable reportError) {
    if (reportError != null) {
      original.addSuppressed(unwrap(reportError));
      return;
    }
    attachSuppressedReportFailure(original, bucket, reportResult);
  }

  private static Throwable unwrap(Throwable error) {
    if (error instanceof CompletionException ce && ce.getCause() != null) {
      return ce.getCause();
    }
    return error;
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable, R> R sneakyThrow(Throwable throwable) throws T {
    throw (T) throwable;
  }

}
