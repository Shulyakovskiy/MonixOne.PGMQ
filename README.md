# MonixOne.Queue.Pgmq

📨 Переиспользуемая PostgreSQL-очередь для .NET 10 на базе PGMQ.

- Отправитель публикует сообщения через `IQueue`.
- Обработчик реализует `IQueueHandler<T>`.
- Доставка — **at-least-once**.
- Невосстанавливаемые сообщения попадают в `<queue>-dlq`.

> ⚠️ Обработчик обязан быть идемпотентным.
> Используйте `QueueEnvelope.Id` как ключ дедупликации,
> например в таблице `processed_messages`.

## 📦 SQL-only provisioning PGMQ

`AddPgmq()` при старте приложения применяет встроенные в пакет SQL-скрипты.
Пакет не скачивает файлы из сети и не запускает `make`.

Под transaction-scoped PostgreSQL advisory lock он:

1. Создаёт `monixone_queue.infrastructure_metadata`.
2. На чистой БД выполняет базовый upstream-скрипт `pgmq.sql` из архива пакета.
3. На уже инициализированной БД читает package-managed версию из metadata.
4. При несовпадении применяет последовательность upstream-скриптов
   `pgmq--X--Y.sql` до версии, встроенной в пакет.
5. Записывает ожидаемую и установленную версии, статус и время проверки.

При следующем запуске миграции не выполняются. После обновления NuGet-пакета
выполняются только недостающие SQL-переходы. Если в архиве нет непрерывного
пути миграций до целевой версии, старт завершается ошибкой без частично
записанной metadata.

Это не использует `CREATE EXTENSION` и `ALTER EXTENSION`: PGMQ управляется
как встроенная SQL-схема пакета, а не как server-side PostgreSQL extension.

SQL-скрипты также поставляются как
`contentFiles/any/any/pgmq/v1.13.0` для аудита.
Исходники находятся в
[infrastructure/pgmq/v1.13.0](infrastructure/pgmq/v1.13.0).

## 🚀 Подключение

```csharp
builder.Services.AddPgmq(builder.Configuration);

builder.Services.AddQueueConsumer<NotificationRequested,
    NotificationRequestedHandler>("Notifications");

builder.Services.AddHealthChecks().AddPgmq();
```

`AddPgmq(IConfiguration)` использует сначала `Queue:ConnectionString`,
затем `ConnectionStrings:{Queue:ConnectionStringName}`.

`AddPgmq(IConfigurationSection)` используйте, когда передаёте секцию
`Queue` напрямую.

```json
{
  "ConnectionStrings": {
    "Queue": "Host=localhost;Database=queue;Username=queue;Password=queue"
  },
  "Queue": {
    "ConnectionStringName": "Queue",
    "Defaults": {
      "BatchSize": 10,
      "VisibilityTimeout": "00:01:00",
      "PollingInterval": "00:00:01",
      "MaxAttempts": 5,
      "Concurrency": 1
    },
    "Consumers": {
      "Notifications": {
        "Queue": "notifications",
        "RetryDelaysSeconds": [5, 30, 120, 600]
      }
    }
  }
}
```

Для переменных окружения используйте
стандартное сопоставление:
`Queue__ConnectionString` и
`Queue__Consumers__Notifications__BatchSize`.

## 📨 Отправитель

Опишите стабильный тип и версию сообщения
через `QueueMessage`.
Не меняйте смысл существующего типа
без повышения версии.

```csharp
[QueueMessage("notification.requested", Version = 1)]
public sealed record NotificationRequested(Guid UserId, string Text);

await queue.SendAsync(
    "notifications",
    new NotificationRequested(userId, text),
    cancellationToken: cancellationToken);
```

`QueueSendOptions.CorrelationId` — прикладная метка.
W3C trace ID берётся из активного `Activity`
и хранится отдельно.

> 💡 Когда запись доменных данных и отправка
> должны быть атомарны,
> используйте транзакционный outbox.

## ⚙️ Обработчик

```csharp
public sealed class NotificationRequestedHandler
    : IQueueHandler<NotificationRequested>
{
    public Task HandleAsync(
        NotificationRequested message,
        CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
```

Для каждого сообщения фоновый обработчик
создаёт отдельный DI scope.
`DbContext` и другие зависимости с областью действия
не разделяются конкурентными обработками.

> ⏱️ `VisibilityTimeout` — срок невидимости сообщения,
> а не ограничение времени обработчика.
> Он должен быть больше обычной продолжительности
> `HandleAsync`.
> Автоматическое продление срока невидимости
> в этой версии не реализовано.

## 🔁 Доставка и повторные попытки

1. `pgmq.read` атомарно забирает сообщение
   и скрывает его на `VisibilityTimeout`.
2. Фоновый обработчик вызывает `HandleAsync`
   без открытой SQL-транзакции.
3. После успешного обработчика выполняется `pgmq.delete`.
   Это подтверждение доставки.
4. При ошибке задаётся следующая задержка повтора.
5. После `MaxAttempts` сообщение атомарно копируется в DLQ
   и удаляется из исходной очереди.

Сбой между успешным `HandleAsync` и `pgmq.delete`
приводит к повторной доставке.
Это нормальное свойство at-least-once доставки,
а не ошибка фонового обработчика.

DLQ содержит исходное сообщение, исходную очередь и ID,
число доставок, время ошибки,
тип исключения и его сообщение.

## 🩺 Проверка здоровья

`AddPgmq()` добавляет проверку здоровья с именем `pgmq`.
Она проверяет доступность PostgreSQL
и версию PGMQ из package-managed metadata.

## 🔐 Безопасность и права

- Не логируйте строку подключения
  и содержимое сообщений.
- Роль, под которой стартует приложение, должна иметь права на создание
  и изменение схемы `pgmq`, её таблиц, типов и функций.
- Не используйте эту роль для прикладных запросов, если у неё
  более широкие права, чем требуются обработчикам очереди.
- Секреты храните в безопасной конфигурации,
  не в отслеживаемых файлах.

## 🧪 Тесты

Модульные тесты не требуют PostgreSQL.
Интеграционные тесты поднимают
изолированный экземпляр Testcontainers
`ghcr.io/pgmq/pg17-pgmq:v1.13.0`
и проверяют provisioning через package initializer.

Тесты не читают локальную конфигурацию,
строки подключения из окружения и общую базу данных.
