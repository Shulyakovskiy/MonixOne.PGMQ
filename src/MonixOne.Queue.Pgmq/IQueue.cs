namespace MonixOne.Queue.Pgmq;

public interface IQueue
{
    Task<long> SendAsync<T>(
        string queue,
        T message,
        QueueSendOptions options,
        CancellationToken cancellationToken = default);

    Task SendBatchAsync<T>(
        string queue,
        IReadOnlyCollection<QueueBatchItem<T>> messages,
        CancellationToken cancellationToken = default);
}
