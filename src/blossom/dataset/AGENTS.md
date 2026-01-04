# Dataset and DataFrame Guidelines

## Entry Points
- `create_dataset(data, engine=DatasetEngine.LOCAL, context=None, spark_session=None)` builds a dataset from an in-memory list of `Schema`.
- `load_dataset(path, engine=DatasetEngine.LOCAL, data_type=DataType.JSON, data_handler=None, context=None, spark_session=None)` loads JSON/JSONL from files or directories.
- `DatasetEngine` supports `LOCAL`, `MULTIPROCESS`, `RAY`, and `SPARK`. `DataType` currently supports `JSON` only.

## Dataset API Summary
`Dataset` is a thin, chainable wrapper over a `DataFrame`. The key API is:
- `map`, `filter`, `transform`, `sort`, `limit`, `shuffle`, `repartition`, `split`, `union`, `cache`, `collect`
- `execute(operators)`: calls `operator.init_context(self.context)` and applies each `Operator` in order.
- `add_metadata`, `drop_metadata` for `Schema.metadata` updates.
- `read_json` / `write_json` for JSONL I/O.
- Aggregations: `aggregate(*AggregateFunc)` plus helpers `sum`, `mean`, `count`, `min`, `max`, `variance`, `stddev`, `unique`.
- `group_by(func, name="group")` returns a `GroupedDataset` with the same aggregate helpers.

Example:
```python
from blossom.dataset import create_dataset, DatasetEngine
dataset = create_dataset(data, engine=DatasetEngine.LOCAL)
result = dataset.filter(fn).map(fn2).aggregate(Count())
```

## DataFrame Engines and Behavior
- `LocalDataFrame`: in-memory list; `repartition` is a no-op with a warning. `read_json` accepts files or directories and reads `.json`/`.jsonl` line-by-line.
- `MultiProcessDataFrame`: uses `ProcessPoolExecutor` + `cloudpickle` for `map`/`filter`/`transform`/`sort`. Functions must be picklable; shared state is not safe. Aggregations/grouping run in-process.
- `RayDataFrame`: wraps Ray Dataset and converts `Schema` to row dicts. `ray.init()` is called if needed. `write_json` uses a JSONL datasink.
- `SparkDataFrame`: wraps Spark RDDs. `read_json` expects line-delimited JSON text; `write_json` writes a Spark text output directory. A `SparkSession` is required.

## DataHandler and Schema Conventions
- `DefaultDataHandler` uses `Schema.from_dict` when a `type` field exists, otherwise wraps dicts in `RowSchema`.
- `DictDataHandler(preserve_metadata=False)` can be used for raw dict workflows.
- Aggregations are built on `AggregateFunc` / `RowAggregateFunc`; multiple aggs return a dict keyed by `agg.name`.

## Extension Notes
- New engines must implement `DataFrame` and `GroupedDataFrame`, then be wired into `loader.py`.
- Keep map/filter/transform functions pure and serializable (especially for `MULTIPROCESS`, `RAY`, and `SPARK`).
