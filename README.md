# ISIT312 data lake prototype

Prototype data lake and warehouse for a dispatch company with two branches,
built on Hadoop, Hive, HBase, Spark and Kafka in one container.

## Layout

| Folder | What is in it |
|---|---|
| `env/` | Container image, configuration, `lake.sh` / `lakectl`, smoke tests |
| `data_prep/` | Export of the real extract, synthetic sources, media stand-ins |
| `data/real/` | Anonymised sample of the company extract (branch A, 16 Nov 2026) |
| `pipelines/` | Batch ingestion, Spark curation and warehouse build, Hive tables and queries, HBase, streaming, tests, experiments |
| `diagrams/` | Architecture and data model diagrams (draw.io sources and PNG) |
| `evidence/` | `run_all.sh`, logs of the full run, screenshots, Superset dashboard |

`data/synthetic/` and `data/media/` are not in git (too big). Recreate them with
`data_prep/02_generate_synthetic.py` and `data_prep/03_generate_media.py`.

## Running it

See `env/README.md` for the container. In short:

```bash
cd env
./lake.sh up
./lake.sh start-all
cd ..
evidence/run_all.sh      # whole prototype from an empty lake, about 60 minutes
```

## Team

| Member | Student no. | GitHub | Part |
|---|---|---|---|
| Amanuel Michael Gunta | 8087088 | [@amg292](https://github.com/amg292) | Platform and environment: container image, service config, smoke tests, Pig install, experiments E7 (replication) and E8 (object storage) |
| Mohammad Basil | 8780158 | [@mgohar1025](https://github.com/mgohar1025) | Data preparation and batch ingestion: export of the real extract, synthetic sources, media stand-ins, raw zone and its Hive tables, findings on the real extract |
| Ahmad Aldabas | 7920003 | [@Ahmadaldabas](https://github.com/Ahmadaldabas) | Curation and warehouse: Spark curation jobs, warehouse build (fact constellation), curated and warehouse Hive tables, OLAP queries, defect test, conceptual and star schema diagrams, screenshots for the report |
| Daria Demenshina | 7916577 | [@rub1d1i](https://github.com/rub1d1i) | Streaming and HBase: GPS replay to Kafka, Structured Streaming job, restart test, HBase tables, media index, experiments E2 to E6, architecture and streaming diagrams, full run and logs |
| Gakam Gakam Jordan Bryan | 8328997 | [@jordan2371](https://github.com/jordan2371) | Engines and dashboard: experiment E1 (MapReduce, Pig, Hive, Spark), report tables, Superset dashboard, ETL diagrams, slide versions of the diagrams and screenshots |
