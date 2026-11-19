package my.app.microbucketj.client.model;

public record LockEntry(
    String name,
    long lockUntilMs,
    long lockedAtMs,
    String lockedBy
) {
}
