# MonixOne.Queue.Pgmq

📨 Переиспользуемая PostgreSQL-очередь для .NET 10 на базе PGMQ.

- Отправитель публикует сообщения через `IQueue`.
- Обработчик реализует `IQueueHandler<T>`.
- Доставка — **at-least-once**.
- Невосстанавливаемые сообщения попадают в `<queue>-dlq`.

> ⚠️ Обработчик обязан быть идемпотентным.
> Используйте `QueueEnvelope.Id` как ключ дедупликации,
> например в таблице `processed_messages`.

## 📦 Установка PGMQ

Используется PGMQ `v1.13.0` (`32c075b`) из
[pgmq/pgmq](https://github.com/pgmq/pgmq).
Исходный архив этого релиза уже входит в NuGet-пакет.

1. Извлеките `pgmq-v1.13.0.tar.gz` из
   `contentFiles/any/any/pgmq/v1.13.0`.
2. Сверьте SHA-256 с `pgmq.lock.json`.
3. Установите extension-файлы для PostgreSQL на целевом сервере:

   ```bash
   tar -xzf pgmq-v1.13.0.tar.gz
   cd pgmq-1.13.0/pgmq-extension
   make install PG_CONFIG=/path/to/pg_config
   ```

4. Выполните ролью миграции или развёртывания
   скрипты строго в порядке:
   `001-create-metadata.sql`, затем `002-install-pgmq.sql`.
5. Запустите приложение.
   Оно проверит установленную версию и запишет статус.

Скрипты поставляются в NuGet-пакете как
`contentFiles/any/any/pgmq/v1.13.0`.
Исходники находятся в
[infrastructure/pgmq/v1.13.0](infrastructure/pgmq/v1.13.0).

> 🔒 Приложение не выполняет `CREATE EXTENSION`,
> `pgmq.create` и иной DDL.
> Владельцем схемы и расширения остаётся
> роль миграции или развёртывания.

При старте в `monixone_queue.infrastructure_metadata` сохраняются:

- ожидаемая и установленная версии;
- статус `installed`, `missing` или `version_mismatch`;
- источник, ревизия и время проверки.

Несовпадение версии останавливает приложение.
Для обновления создайте новую версионированную папку
со скриптами.
Опубликованные скрипты не переписывайте.

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
и версию установленного расширения.

## 🔐 Безопасность и права

- Не логируйте строку подключения
  и содержимое сообщений.
- Выдайте прикладной роли только доступ к PGMQ и
  `monixone_queue.infrastructure_metadata`.
- Не выдавайте прикладной роли DDL-права
  на расширение, схему или таблицу.
- Секреты храните в безопасной конфигурации,
  не в отслеживаемых файлах.

## 🧪 Тесты

Модульные тесты не требуют PostgreSQL.
Интеграционные тесты поднимают
изолированный экземпляр Testcontainers
`ghcr.io/pgmq/pg17-pgmq:v1.13.0`
и применяют пакетные скрипты развёртывания.

Тесты не читают локальную конфигурацию,
строки подключения из окружения и общую базу данных.
