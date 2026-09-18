namespace MonixOne.Queue;

public interface IQueueHandler<in T>
{
    Task HandleAsync(
        T message,
        CancellationToken cancellationToken);
}
