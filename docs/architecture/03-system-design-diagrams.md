# System Design Diagrams

Three views of the platform: data flow, components, deployment. Diagrams use Mermaid so they render inline on GitHub and in the report.

## 1. Data Flow Diagram (Level 1)

Two ingestion paths flow into one warehouse: a **Lambda batch arm** (top) for outlets that only expose batch APIs and a **Kappa stream arm** (bottom) that publishes through Kafka. Downstream cleaning, embedding, topic modelling, and aggregation are shared.

```mermaid
flowchart LR
    %% External entities
    SRC[/"News APIs<br/>NewsAPI · GDELT"/]:::ext
    RSS[/"RSS · Google News<br/>Scraped outlets<br/>DE US IT MM KZ"/]:::ext
    ANL[/"Analyst<br/>(dashboard user)"/]:::ext

    %% Lambda path
    subgraph LAMBDA ["Lambda (batch arm)"]
        direction LR
        P1B(["P1.b<br/>Batch ingest<br/>dlt + BS4"]):::proc
        DS1[("DS1<br/>DuckDB<br/>raw lake")]:::store
        P1B --> DS1
    end

    %% Kappa path
    subgraph KAPPA ["Kappa (stream arm)"]
        direction LR
        P1A(["P1.a<br/>Stream ingest<br/>(producers)"]):::proc
        DS0[("DS0<br/>Kafka topic<br/>unified_news_topic")]:::store
        P1C(["P1.c<br/>Consume + load<br/>(dlt consumer)"]):::proc
        P1A --> DS0 --> P1C
    end

    %% Shared downstream
    P2(["P2<br/>Clean + dedup"]):::proc
    P3(["P3<br/>Embed + topic-model"]):::proc
    P4(["P4<br/>Aggregate<br/>country × topic × time"]):::proc
    P5(["P5<br/>Serve queries"]):::proc
    DS2[("DS2<br/>Vector store<br/>embeddings")]:::store
    DS3[("DS3<br/>ClickHouse<br/>modelled DWH")]:::store

    %% Edges in
    SRC --> P1B
    SRC --> P1A
    RSS --> P1B
    RSS --> P1A

    %% Edges through
    P1C  -- "raw rows" --> DS3
    DS1  -- "raw rows" --> P2
    DS3  -- "raw rows" --> P2
    P2   --> P3
    P3   -- "vectors" --> DS2
    P3   -- "enriched rows" --> P4
    DS2  -- "neighbours" --> P4
    P4   -- "country_topic_daily" --> DS3
    DS3  --> P5
    P5   -- "narrative views" --> ANL

    classDef ext fill:#ffe4d2,stroke:#d96f3a,color:#3a1f0c;
    classDef proc fill:#f2e9ff,stroke:#7b4fbf,color:#1d0c3a;
    classDef store fill:#dfe9ff,stroke:#1f4cbf,color:#0c1d3a;
    classDef path fill:#fafaff,stroke:#7b4fbf,stroke-dasharray: 5 4,color:#1d0c3a;
    class LAMBDA,KAPPA path;
```

## 2. Component Diagram (UML)

Components are stacked top to bottom along the data lifecycle: ingest, transport, process, store, serve. Solid arrows are "uses" / "writes to"; dashed are read paths.

```mermaid
flowchart TB
    subgraph ING ["Ingestion"]
        direction LR
        DLTSRC["«component»<br/>dlt REST sources<br/>NewsAPI · GDELT"]
        BS4["«component»<br/>RSS + BS4 scrapers"]
        DLTPIPE["«component»<br/>dlt batch pipeline<br/>(merge + schema)"]
        KPROD["«component»<br/>Kafka producers<br/>RSS · NewsAPI · BBC · scrapers"]
    end

    subgraph TRANSPORT ["Transport"]
        direction LR
        KBROKER[("«message broker»<br/>Kafka<br/>unified_news_topic")]
        KCONS["«component»<br/>dlt Kafka consumer<br/>(chunked batches)"]
    end

    subgraph PROC ["Processing"]
        direction LR
        CLEAN["«component»<br/>Cleaner / Deduper"]
        EMB["«component»<br/>Embedding service<br/>sentence-transformers"]
        TOPIC["«component»<br/>Topic modeller<br/>BERTopic / LDA"]
        AGG["«component»<br/>Aggregator<br/>(SQL)"]
    end

    subgraph STORE ["Storage"]
        direction LR
        DUCK[("«database»<br/>DuckDB<br/>raw lake")]
        VEC[("«database»<br/>Vector store<br/>DuckDB / Qdrant")]
        CH[("«database»<br/>ClickHouse<br/>modelled DWH")]
    end

    subgraph SERVE ["Serving"]
        direction LR
        QAPI["«component»<br/>Query layer<br/>(ClickHouse driver)"]
        STREAMLIT["«component»<br/>Streamlit<br/>live Kappa feed"]
        MARIMO["«component»<br/>marimo<br/>narrative divergence"]
    end

    %% Lambda wiring
    DLTSRC --> DLTPIPE
    BS4    --> DLTPIPE
    DLTPIPE --> DUCK

    %% Kappa wiring
    KPROD --> KBROKER
    KBROKER --> KCONS
    KCONS --> CH

    %% Processing wiring
    DUCK -. read .-> CLEAN
    CH   -. read .-> CLEAN
    CLEAN --> DUCK
    CLEAN -. read .-> EMB
    CLEAN -. read .-> TOPIC
    EMB   --> VEC
    AGG   -. read .-> DUCK
    AGG   -. read .-> VEC
    AGG   --> CH

    %% Serving wiring
    CH -. read .-> QAPI
    QAPI --> STREAMLIT
    QAPI --> MARIMO

    classDef sub fill:#f7f2ff,stroke:#7b4fbf,stroke-dasharray: 5 4;
    class ING,TRANSPORT,PROC,STORE,SERVE sub;
```

## 3. Deployment Diagram (UML)

Three nodes: the dev laptop (Python processes), a Docker host (broker + DWH), and external APIs. Dashboards run as `uv` processes on the dev laptop, not inside containers.

```mermaid
flowchart LR
    subgraph DEV ["«device» Dev laptop (macOS)"]
        direction TB
        ART_BATCH["«artifact»<br/>pipelines/ingest_*.py<br/>(batch arm, uv)"]
        ART_PRODUCER["«artifact»<br/>pipelines/kafka/producer_*.py<br/>(uv)"]
        ART_CONSUMER["«artifact»<br/>pipelines/kafka/consumer_to_clickhouse.py<br/>(uv)"]
        ART_PROC["«artifact»<br/>processing/*.py<br/>(embeddings + topics, uv)"]
        ART_DUCK["«artifact»<br/>news.duckdb<br/>(file)"]
        ART_STREAMLIT["«artifact»<br/>dashboard/streamlit_app.py<br/>(uv, :8501)"]
        ART_MARIMO["«artifact»<br/>dashboard/app.py<br/>(uv marimo, :2718)"]
    end

    subgraph DOCKER ["«device» Docker host (local or HWR VPS)"]
        direction TB
        ART_ZK["«artifact»<br/>zookeeper<br/>(:2181)"]
        ART_KAFKA["«artifact»<br/>kafka broker<br/>(:9092)"]
        ART_CH["«artifact»<br/>clickhouse-server<br/>(:8123 HTTP, :9000 native)"]
        ART_ZK --- ART_KAFKA
    end

    subgraph EXTAPI ["«external» News providers"]
        direction TB
        ART_NEWS["«artifact»<br/>NewsAPI"]
        ART_GDELT["«artifact»<br/>GDELT 2.0 DOC"]
        ART_RSS["«artifact»<br/>RSS · Google News · scraped sites"]
    end

    subgraph CLIENT ["«device» Analyst browser"]
        ART_BROWSER["«artifact»<br/>Chrome / Safari"]
    end

    %% Egress to providers
    ART_BATCH    -- "HTTPS :443" --> ART_NEWS
    ART_BATCH    -- "HTTPS :443" --> ART_GDELT
    ART_BATCH    -- "HTTPS :443" --> ART_RSS
    ART_PRODUCER -- "HTTPS :443" --> ART_NEWS
    ART_PRODUCER -- "HTTPS :443" --> ART_RSS

    %% Lambda arm to DuckDB file
    ART_BATCH -- "file I/O" --> ART_DUCK
    ART_PROC  -- "file I/O" --> ART_DUCK

    %% Kappa arm through broker
    ART_PRODUCER -- "kafka :9092" --> ART_KAFKA
    ART_KAFKA    -- "kafka :9092" --> ART_CONSUMER
    ART_CONSUMER -- "HTTP :8123" --> ART_CH

    %% Processing + serving against ClickHouse
    ART_PROC      -- "HTTP :8123" --> ART_CH
    ART_STREAMLIT -- "HTTP :8123" --> ART_CH
    ART_MARIMO    -- "HTTP :8123" --> ART_CH

    %% Browser to dashboards
    ART_BROWSER -- "HTTP :8501" --> ART_STREAMLIT
    ART_BROWSER -- "HTTP :2718" --> ART_MARIMO

    classDef node fill:#dfe9ff,stroke:#1f4cbf,color:#0c1d3a;
    classDef art fill:#f0f4ff,stroke:#1f4cbf,color:#0c1d3a;
    class DEV,DOCKER,EXTAPI,CLIENT node;
    class ART_BATCH,ART_PRODUCER,ART_CONSUMER,ART_PROC,ART_DUCK,ART_STREAMLIT,ART_MARIMO,ART_ZK,ART_KAFKA,ART_CH,ART_NEWS,ART_GDELT,ART_RSS,ART_BROWSER art;
```

## Reading guide

- **DFD (1)** answers "what data goes where" without committing to technology. The Kappa and Lambda subgraphs are the two ingestion shapes the project runs in parallel.
- **Component (2)** answers "what software is involved" and groups by lifecycle stage. Kafka sits in a dedicated transport layer to make the decoupling between producers and consumer explicit.
- **Deployment (3)** answers "where does this run" and how the boxes talk over the wire. Local dev shape: most processes are `uv` on the laptop; only the broker + DWH live in Docker.
