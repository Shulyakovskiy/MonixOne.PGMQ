namespace MonixOne.Queue;

public interface IQueue
{
    Task<long> SendAsync<T>(
        string queue,
        T message,
        QueueSendOptions? options = null,
        CancellationToken cancellationToken = default);

    Task SendBatchAsync<T>(
        string queue,
        IReadOnlyCollection<T> messages,
        QueueSendOptions? options = null,
        CancellationToken cancellationToken = default);
}
