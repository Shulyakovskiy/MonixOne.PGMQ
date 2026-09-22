using System.Data.Common;

namespace MonixOne.Queue.Pgmq;

public interface IQueue
{
    Task<long> SendAsync<T>(
        string queue,
        T message,
        QueueSendOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a message to PGMQ through the transaction that owns the domain write.
    /// The caller commits or rolls back both changes.
    /// </summary>
    Task<long> SendAsync<T>(
        string queue,
        T message,
        QueueSendOptions options,
        DbTransaction transaction,
        CancellationToken cancellationToken = default);

    Task SendBatchAsync<T>(
        string queue,
        IReadOnlyCollection<QueueBatchItem<T>> messages,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds all messages through the transaction that owns the domain write.
    /// The caller commits or rolls back the whole batch together with its domain changes.
    /// </summary>
    Task SendBatchAsync<T>(
        string queue,
        IReadOnlyCollection<QueueBatchItem<T>> messages,
        DbTransaction transaction,
        CancellationToken cancellationToken = default);
}
