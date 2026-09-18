using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace MonixOne.Queue.Pgmq;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddPgmq(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var section = configuration.GetSection(PgmqOptions.SectionName);
        return AddPgmqCore(services, options =>
        {
            section.Bind(options);
            options.ConnectionString ??= configuration.GetConnectionString(options.ConnectionStringName);
        });
    }

    public static IServiceCollection AddPgmq(
        this IServiceCollection services,
        IConfigurationSection section)
    {
        ArgumentNullException.ThrowIfNull(section);
        return AddPgmqCore(services, section.Bind);
    }

    public static IServiceCollection AddQueueConsumer<TMessage, THandler>(this IServiceCollection services, string consumerName)
        where THandler : class, IQueueHandler<TMessage>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerName);
        services.AddScoped<IQueueHandler<TMessage>, THandler>();
        services.AddSingleton<IHostedService>(serviceProvider => new PgmqWorker<TMessage>(
            consumerName,
            serviceProvider.GetRequiredService<PgmqClient>(),
            serviceProvider.GetRequiredService<QueueJsonSerializer>(),
            serviceProvider.GetRequiredService<IServiceScopeFactory>(),
            serviceProvider.GetRequiredService<IOptions<PgmqOptions>>(),
            serviceProvider.GetRequiredService<ILogger<PgmqWorker<TMessage>>>()));
        return services;
    }

    public static IHealthChecksBuilder AddPgmq(
        this IHealthChecksBuilder builder,
        string? name = null,
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
    {
        return builder.Add(new HealthCheckRegistration(name ?? "pgmq", serviceProvider => new PgmqHealthCheck(
            serviceProvider.GetRequiredService<NpgsqlDataSource>()), failureStatus, tags));
    }

    private static IServiceCollection AddPgmqCore(
        IServiceCollection services,
        Action<PgmqOptions> configure)
    {
        services.AddOptions<PgmqOptions>()
            .Configure(configure)
            .Services
            .AddSingleton<IValidateOptions<PgmqOptions>, PgmqOptionsValidator>();
        services.AddOptions<PgmqOptions>().ValidateOnStart();
        services.TryAddSingleton<NpgsqlDataSource>(serviceProvider =>
        {
            // One data source owns Npgsql's host-wide connection pool; queue operations lease pooled connections.
            var connectionString = serviceProvider.GetRequiredService<IOptions<PgmqOptions>>().Value.ConnectionString!;
            return NpgsqlDataSource.Create(connectionString);
        });
        services.TryAddSingleton<PgmqClient>();
        services.TryAddSingleton<QueueJsonSerializer>();
        services.TryAddSingleton<IQueue, PgmqQueue>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, PgmqInitializer>());
        return services;
    }
}
