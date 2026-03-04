# Simple Log Period Summarizer

Минимальная версия с 3 интерфейсами.

## 3 интерфейса

1. `db_fetch_page(...) -> list[dict]`
- Ты реализуешь запрос в БД.
- Я в этом интерфейсе передаю `limit` и `offset`.
- Также передаю `period_start`, `period_end` и `columns`.

2. `llm_call(prompt: str) -> str`
- Текстовый промпт на вход.
- Текстовый ответ на выход.

3. `summarize_period(...) -> SummarizationResult`
- Главный вызов суммаризации за период.

Реализация: `llm_log_summarizer/simple_period_summarizer.py`

## Как совместить period + limit/offset

Используй такой SQL-паттерн:

```sql
SELECT <columns>
FROM logs
WHERE ts >= :period_start
  AND ts < :period_end
ORDER BY ts, id
LIMIT :limit OFFSET :offset;
```

Почему так:
- `WHERE` фиксирует период.
- `LIMIT/OFFSET` листает страницы внутри этого периода.
- `ORDER BY ts, id` делает пагинацию стабильной.

Важно:
- Если в таблицу параллельно пишутся новые строки, offset-пагинация может "плавать".
- Для максимальной надежности используй snapshot/транзакцию или keyset pagination.

## Поток работы

1. Fetch page (`limit/offset`) в рамках периода.
2. Разбить страницу на LLM-чанки (`llm_chunk_rows`).
3. На MAP-этапе LLM ищет именно проблемы (ошибки, таймауты, деградации), не общий обзор.
4. Если chunk summary много, сделать reduce в несколько раундов.
5. На REDUCE-этапе LLM ранжирует TOP_PROBLEMS и формирует приоритетные действия.
6. Вернуть финальный summary + метрики.

## Пример

Смотри: `example_usage.py`
