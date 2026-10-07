# MonixOne.Queue.Pgmq

PostgreSQL-очередь для .NET 10 на базе PGMQ.

| Задача | API |
| --- | --- |
| Отправить сообщение | `IQueue` |
| Обработать сообщение | `IQueueHandler<T>` |
| Хранить ошибки всех очередей | Общая DLQ `monixone_dlq` |
| Читать сообщения из DLQ | `IQueue.GetDeadLetterMessagesAsync` |

**Доставка — at-least-once:** сообщение может прийти повторно.

> Для каждого логического сообщения задавайте стабильный `IdempotencyKey`.
> При повторной отправке используйте тот же ключ.

## 🚀 Быстрый старт

### Регистрация

```csharp
builder.Services.AddPgmq(builder.Configuration);

builder.Services.AddQueueConsumer<NotificationRequested,
    NotificationRequestedHandler>("Notifications");

builder.Services.AddHealthChecks().AddPgmq();
```

При старте пакет создаёт настроенные очереди и одну общую DLQ.

`AddPgmq(IConfigurationSection)` используйте для готовой секции `Queue`.

### Конфигурация

```json
{
  "ConnectionStrings": {
    "Queue": "Host=localhost;Database=queue;Username=queue;Password=queue"
  },
  "Queue": {
    "ConnectionStringName": "Queue",
    "DeadLetterQueue": "monixone_dlq",
    "CompletedIdempotencyRetention": "1.00:00:00",
    "IdempotencyCleanupInterval": "01:00:00",
    "IdempotencyCleanupBatchSize": 1000,
    "Defaults": {
      "BatchSize": 10,
      "VisibilityTimeout": "00:01:00",
      "VisibilityRenewalInterval": "00:00:20",
      "MaxProcessingDuration": "00:02:00",
      "HandlerCancellationGracePeriod": "00:00:05",
      "QueueOperationTimeout": "00:00:05",
      "IdempotencyLease": "00:02:30",
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

Настройки из `Defaults` применяются ко всем consumers.
Значения в `Consumers:<имя>` переопределяют их для отдельного обработчика.

Строка подключения выбирается в таком порядке:

1. `Queue:ConnectionString`.
2. `ConnectionStrings:{Queue:ConnectionStringName}`.

Примеры переменных окружения:

- `Queue__ConnectionString`.
- `Queue__Consumers__Notifications__BatchSize`.

## 📨 Отправка сообщений

```csharp
[QueueMessage("notification.requested", Version = 1)]
public sealed record NotificationRequested(Guid UserId, string Text);

await queue.SendAsync(
    "notifications",
    new NotificationRequested(userId, text),
    new QueueSendOptions
    {
        IdempotencyKey = "notification:3f5d8f0a-0a65-4bfb-9c17-c0851ba4dc0f"
    },
    cancellationToken: cancellationToken);
```

Правила для контракта:

- Указывайте стабильные тип и версию через `QueueMessage`.
- При изменении смысла сообщения повышайте версию.
- Сохраняйте `IdempotencyKey` при повторной отправке.
- Ключ уникален в пределах consumer-а.
- Для batch API передавайте `QueueSendOptions` в каждом `QueueBatchItem<T>`.

`SendBatchAsync` проверяет весь batch до записи и вставляет его одним вызовом `pgmq.send_batch`.
Сообщения добавляются атомарно, включая вариант с транзакцией вызывающего кода.

| Поле | Назначение |
| --- | --- |
| `CorrelationId` | Прикладная метка для связи операций |
| Trace ID | W3C trace ID из текущего `Activity`, хранится отдельно |

### Отправка в транзакции

Если доменные данные и PGMQ находятся в одной PostgreSQL БД,
передайте текущую транзакцию в `SendAsync`.

Доменная запись и сообщение тогда фиксируются или откатываются вместе.

```csharp
await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);

db.Notifications.Add(notification);
await db.SaveChangesAsync(cancellationToken);

await queue.SendAsync(
    "notifications",
    new NotificationRequested(notification.UserId, notification.Text),
    new QueueSendOptions { IdempotencyKey = $"notification:{notification.Id}" },
    transaction.GetDbTransaction(),
    cancellationToken);

await transaction.CommitAsync(cancellationToken);
```

`GetDbTransaction()` доступен в `Microsoft.EntityFrameworkCore.Storage`.

Если данные и очередь находятся в разных БД,
используйте transactional outbox.
Он также нужен для надёжной связи доменной транзакции с внешней публикацией.

## ⚙️ Обработка сообщений

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

В `HandleAsync` выполняйте прикладную обработку или публикацию.
Передавайте `CancellationToken` в БД, сетевые вызовы и задержки.

Для каждой доставки создаётся отдельный DI scope.
`DbContext` и другие scoped-зависимости не разделяются между обработчиками.

Handler выбирается по имени consumer-а.
Разные consumer-ы одного типа сообщения могут использовать разные handler-ы.

### Основные настройки

| Параметр | По умолчанию | Назначение |
| --- | --- | --- |
| `BatchSize` | 10 | Верхняя граница количества сообщений за одно чтение |
| `Concurrency` | 1 | Число параллельных обработчиков в одном worker-е |
| `PollingInterval` | 1 с | Пауза после пустого чтения или ошибки чтения |
| `MaxAttempts` | 5 | Счётчик доставок, после которого ошибка приводит к DLQ |

Worker читает не больше `min(BatchSize, Concurrency)` сообщений.

При стандартных значениях он забирает **одно сообщение** за чтение.
Сообщения не ждут обработки внутри локального batch.

## ⏱️ Visibility и таймауты

### Значения по умолчанию

| Параметр | Значение | Что ограничивает |
| --- | --- | --- |
| `VisibilityTimeout` | 60 с | Время невидимости сообщения после чтения и продления |
| `VisibilityRenewalInterval` | 20 с | Интервал heartbeat |
| `MaxProcessingDuration` | 120 с | Общую длительность одной попытки |
| `HandlerCancellationGracePeriod` | 5 с | Ожидание отменённого handler-а, его scope и callbacks |
| `QueueOperationTimeout` | 5 с | Операцию PGMQ и ожидание остановки heartbeat |
| `IdempotencyLease` | 150 с* | Владение логическим сообщением |

\* Если lease не указан, он рассчитывается из лимитов consumer-а:

```text
IdempotencyLease = MaxProcessingDuration
                  + max(HandlerCancellationGracePeriod, QueueOperationTimeout)
                  + QueueOperationTimeout
                  + 20 секунд запаса
```

Для стандартных настроек: **120 + 5 + 5 + 20 = 150 секунд**.

### Как выбрать visibility

Учитывайте время полного штатного цикла:

```text
pgmq.read → обработка / публикация → completed → pgmq.delete
```

`VisibilityTimeout` должен покрывать этот цикл с запасом,
включая подтверждение брокера и внутренние retries.

### Как работает продление

1. Перед запуском handler-а worker проверяет владение доставкой.
2. Продлевает visibility, затем запускает handler и heartbeat.
3. Heartbeat обновляет visibility каждые `VisibilityRenewalInterval`.
4. На пределе `MaxProcessingDuration` обработка отменяется.

Продление ограничено абсолютным deadline:

```text
deadline      = last_read_at + MaxProcessingDuration
visible_until = min(database_now + VisibilityTimeout, deadline)
```

Deadline берётся из времени PostgreSQL.
Локальное ожидание использует монотонные часы и включает ожидание соединения для чтения.

**Heartbeat продлевает только visibility. `IdempotencyLease` остаётся фиксированным.**

### Проверка конфигурации

При старте проверяются условия:

```text
MaxProcessingDuration >= VisibilityTimeout

VisibilityRenewalInterval + QueueOperationTimeout < VisibilityTimeout

IdempotencyLease > (
    MaxProcessingDuration
    + max(HandlerCancellationGracePeriod, QueueOperationTimeout)
    + QueueOperationTimeout
)
```

> **Обновление старой конфигурации:** явно заданный lease 120 секунд
> замените на 150 секунд либо удалите `IdempotencyLease` для автоматического расчёта.

## 🛑 Отмена и освобождение ресурсов

### Поведение worker-а

| Сценарий | Действие |
| --- | --- |
| Успех | Остановить heartbeat, зафиксировать `completed`, удалить сообщение |
| Ошибка handler-а | Назначить retry с backoff; при исчерпании попыток — DLQ |
| Deadline попытки | Отменить handler и heartbeat, затем выполнить retry/DLQ |
| Shutdown | После остановки handler-а вернуть сообщение и освободить lease |
| Потеря владения | Отменить handler; состояние PGMQ-доставки не менять |

Retry, DLQ и освобождение lease выполняются после завершения handler-а и его scope.
После фиксации `completed` допускается только подтверждение доставки.

### Что освобождается

- Handler и heartbeat останавливаются параллельно.
- Остановка handler-а ограничена `HandlerCancellationGracePeriod`.
- Остановка heartbeat ограничена `QueueOperationTimeout`.
- Cancellation callbacks выполняются асинхронно и входят в grace period.
- Соединения и блокировки удерживаются только внутри коротких SQL-операций.
- При shutdown visibility и lease освобождаются в одной транзакции.

SQL-операции имеют собственный бюджет отмены.
Он включает ожидание connection pool и блокировок PostgreSQL.

### Источники отмены

| Операция | Токен / ограничение |
| --- | --- |
| Отправка, startup, health check | Токен вызывающего кода |
| Чтение и polling | Остановка host-а; чтение также ограничено `QueueOperationTimeout` |
| Handler | Остановка host-а, deadline или потеря владения |
| Heartbeat, completed и ack | Токен обработки и `QueueOperationTimeout` |
| Очистка завершённых ключей | Остановка host-а и отдельный таймаут для каждого batch |
| Retry, DLQ, release | Новый токен с `QueueOperationTimeout` после остановки обработки |

Для завершающих SQL-переходов уже отменённый токен обработки не используется.
Их отдельный таймаут позволяет освободить доставку и lease при shutdown или deadline.

Перед retry worker дожидается остановки heartbeat.
Позднее продление не может перезаписать задержку повтора.

### Если обработка зависла

Если handler, scope, callback или heartbeat не завершился в свой бюджет:

1. Worker записывает событие уровня **Critical**.
2. Вызывает `IHostApplicationLifetime.StopApplication()`.
3. Сохраняет lease до истечения срока и не подтверждает сообщение.

**Настройте перезапуск процесса supervisor-ом.**

Scope освобождается после фактического завершения использующей его задачи.
.NET не может безопасно прервать произвольный handler внутри процесса.

### Защита от устаревшего worker-а

В коротких транзакциях проверяются:

- `lease_token` и срок idempotency lease;
- текущий `read_ct` — поколение PGMQ-доставки.

После потери владения worker не продлевает сообщение и не переводит его в retry/DLQ.

## 🔁 Повторы, идемпотентность и DLQ

### Когда возможна повторная обработка

| Момент сбоя | Следующая доставка |
| --- | --- |
| До фиксации `completed` | Handler может выполниться повторно |
| После фиксации `completed` | Дубликат пропускает handler и повторяет подтверждение |

Ошибка удаления после `completed` не отправляет сообщение в retry/DLQ.

Для внешней публикации используйте стабильный message ID и дедупликацию.
Подтверждение брокера может потеряться после принятия сообщения.

Строгую атомарность бизнес-эффекта обеспечивайте прикладной идемпотентностью
в одной транзакции с доменными изменениями.

### Retry и DLQ

- По умолчанию задержки повторов: **5, 30, 120, 600 секунд**.
- После конца списка повторно используется последняя задержка.
- `MaxAttempts` проверяется по счётчику доставок PGMQ.
- При исчерпании попыток ошибочная доставка попадает в общую `Queue:DeadLetterQueue`.
- Запись в DLQ, удаление исходного сообщения и освобождение lease атомарны.

В DLQ сохраняются:

- исходное сообщение, очередь, топик, consumer и исходный ID;
- число доставок и время ошибки;
- тип и текст исключения.

### Единое чтение DLQ

Все ошибки хранятся в **одной PGMQ-очереди** `monixone_dlq`.
Имя можно задать через `Queue:DeadLetterQueue`.

| Поле | Значение |
| --- | --- |
| `Id` | ID записи в общей DLQ; курсор для страниц |
| `Queue` | Название исходной очереди |
| `Topic` | Стабильный тип сообщения из `QueueMessage` / поля `type` |
| `ConsumerName` | Consumer, завершивший обработку ошибкой |
| `MessageId` | ID сообщения в исходной очереди |
| `OriginalMessage` | Исходный JSON envelope |

Один метод читает сообщения **из всех очередей и топиков**:

```csharp
var page = await queue.GetDeadLetterMessagesAsync(
    cancellationToken: cancellationToken);
```

Для выборки по очереди или топику задайте фильтры:

```csharp
var page = await queue.GetDeadLetterMessagesAsync(
    new QueueDeadLetterQuery
    {
        Queue = "notifications",
        Topic = "notification.requested",
        PageSize = 100,
        AfterId = 0
    },
    cancellationToken);
```

- Результат находится в `page.Messages`.
- Для следующей страницы передайте `page.NextAfterId` в `AfterId` с теми же фильтрами.
- Если `NextAfterId == null`, текущая выборка закончилась.
- Размер страницы — 100 по умолчанию, максимум 500.
- Без фильтров возвращаются записи всех очередей и топиков.

Чтение включает невидимые записи, сохраняет `read_ct` и visibility и не удаляет сообщения.
Каждая страница отражает текущее состояние DLQ и освобождает соединение до возврата результата.
Для фильтров по очереди и топику создаются отдельные индексы.

### Обновление с отдельных DLQ

При старте записи из `<queue>-dlq` настроенных consumer-ов переносятся в общую DLQ.
Топик восстанавливается из исходного envelope; если тип определить нельзя, используется `unknown`.

Перенос выполняется по 100 записей за транзакцию с отдельным бюджетом отмены.
Повторный запуск продолжает перенос оставшихся записей.
Старые таблицы сохраняются.

При обновлении остановите worker-ы старой версии до переноса,
чтобы они завершили запись в отдельные DLQ.

### Хранение завершённых ключей

| Параметр | По умолчанию |
| --- | --- |
| `CompletedIdempotencyRetention` | 24 часа |
| `IdempotencyCleanupInterval` | 1 час |
| `IdempotencyCleanupBatchSize` | 1000 записей |

Очистка запускается при старте, затем по интервалу.
Завершённые ключи подавляют поздние дубликаты в пределах срока хранения.

После падения процесса новая обработка может ждать истечения активного lease.

## 📦 Подготовка PostgreSQL

`AddPgmq()` применяет локальные SQL-скрипты при старте приложения.

| Состояние БД | Действие |
| --- | --- |
| Чистая БД | Применить базовый `pgmq.sql` |
| Версия совпадает | Пропустить миграции |
| Версия пакета новее | Применить недостающие `pgmq--X--Y.sql` по порядку |
| Нет непрерывного пути миграций | Откатить стартовую транзакцию и завершить запуск ошибкой |

Инициализация выполняется под transaction-scoped advisory lock.
Перед изменениями проверяется SHA-256 встроенного архива.

Версия и статус хранятся в `monixone_queue.infrastructure_metadata`.
PGMQ устанавливается как SQL-схема; `CREATE EXTENSION` и `ALTER EXTENSION` не требуются.

SQL-файлы доступны в пакете по пути `contentFiles/any/any/pgmq/v1.13.0`.

Подробнее: [SQL-скрипты и обновление](infrastructure/pgmq/v1.13.0/README.md).

## 🩺 Проверка здоровья

`AddHealthChecks().AddPgmq()` регистрирует проверку `pgmq`.

Она проверяет:

- доступность PostgreSQL;
- установленную версию PGMQ в metadata.

## 🔐 Права и секреты

- Роли инициализации нужны права на создание и изменение схемы `pgmq`.
- Прикладным подключениям выдавайте только необходимые права.
- Храните секреты вне отслеживаемых файлов.
- Не логируйте строку подключения и содержимое сообщений.

## 🧪 Тесты

| Вид тестов | Окружение |
| --- | --- |
| Модульные | PostgreSQL не требуется |
| Интеграционные | Изолированный PostgreSQL через Testcontainers |

Образ для интеграционных тестов: `ghcr.io/pgmq/pg17-pgmq:v1.13.0`.

Тесты проверяют пакетную инициализацию и обработку сообщений.
Они используют собственную БД и тестовую конфигурацию.
