namespace MonixOne.Queue.Pgmq;

public interface IQueueHandler<in T>
{
    Task HandleAsync(
        T message,
        CancellationToken cancellationToken);
}
