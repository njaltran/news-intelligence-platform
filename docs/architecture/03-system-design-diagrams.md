# System Design Diagrams

## 1. Data Flow Diagram (Level 1)

```mermaid
flowchart LR
    %% External entities (rectangles)
    SRC[/"News APIs<br/>NewsAPI, GDELT"/]
    RSS[/"RSS + Scraped sites<br/>DE, US, IT, MM, KZ"/]
    ANL[/"Analyst<br/>(dashboard user)"/]

    %% Processes (rounded)
    P1(["P1<br/>Producers<br/>(dlt REST + BS4,<br/>publish unified msgs)"])
    P1B(["P1.5<br/>dlt Kafka consumer<br/>(drain + aggregate sources)"])
    P2(["P2<br/>Clean &amp; Dedup"])
    P3(["P3<br/>Embed &amp; Topic-Model"])
    P4(["P4<br/>Aggregate<br/>country x topic x time"])
    P5(["P5<br/>Serve Query API"])

    %% Data stores
    DS0[("DS0<br/>Kafka topic<br/>unified_news_topic")]
    DS1[("DS1<br/>ClickHouse<br/>raw lake")]
    DS2[("DS2<br/>Vector store<br/>(Qdrant)")]
    DS3[("DS3<br/>ClickHouse<br/>modelled DWH")]

    SRC -- "raw articles (JSON)" --> P1
    RSS -- "HTML + RSS items (high velocity)" --> P1
    P1  -- "unified messages" --> DS0
    DS0 -- "stream consumed in batches" --> P1B
    P1B -- "raw rows" --> DS1
    DS1 -- "raw rows" --> P2
    P2  -- "cleaned, deduped rows" --> P3
    P3  -- "vectors + topic IDs" --> DS2
    DS2 -- "neighbour vectors" --> P4
    P4  -- "country_topic_daily, divergence" --> DS3
    DS3 -- "aggregates" --> P5
    P5  -- "narrative divergence views" --> ANL

    classDef ext fill:#ffe4d2,stroke:#d96f3a,color:#3a1f0c;
    classDef proc fill:#f2e9ff,stroke:#7b4fbf,color:#1d0c3a;
    classDef store fill:#dfe9ff,stroke:#1f4cbf,color:#0c1d3a;
    class SRC,RSS,ANL ext;
    class P1,P1B,P2,P3,P4,P5 proc;
    class DS0,DS1,DS2,DS3 store;
```

## 2. Component Diagram (UML)

```mermaid
flowchart TB
    subgraph ING ["Ingestion (subsystem)"]
        direction TB
        DLTSRC["«component»<br/>dlt REST source<br/>(NewsAPI, GDELT)"]
        BS4["«component»<br/>BeautifulSoup<br/>scraper / RSS"]
        KPROD["«component»<br/>Kafka producers<br/>(RSS, BBC, NewsAPI, scrapers)"]
        KBROKER[("«broker»<br/>Kafka<br/>unified_news_topic")]
        KCONS["«component»<br/>dlt Kafka consumer<br/>(aggregate sources, chunked)"]
    end

    subgraph PROC ["Processing (subsystem)"]
        direction TB
        CLEAN["«component»<br/>Cleaner / Deduper<br/>(SQL on ClickHouse)"]
        EMB["«component»<br/>Embedding Service<br/>(sentence-transformers)"]
        TOPIC["«component»<br/>Topic Modeller<br/>(BERTopic / LDA)"]
        AGG["«component»<br/>Aggregator<br/>(dbt-style SQL)"]
    end

    subgraph STORE ["Storage"]
        direction TB
        CH_RAW["«component»<br/>ClickHouse<br/>(raw lake)"]
        VEC["«component»<br/>Vector store<br/>(Qdrant)"]
        CH["«component»<br/>ClickHouse<br/>(modelled DWH)"]
    end

    subgraph SERVE ["Serving"]
        direction TB
        STREAMLIT["«component»<br/>Streamlit dashboard<br/>(live feed + narrative divergence)"]
        QAPI["«component»<br/>Query layer<br/>(ClickHouse driver)"]
    end

    DLTSRC -. "uses" .-> KPROD
    BS4    -. "uses" .-> KPROD
    KPROD  -. "publishes" .-> KBROKER
    KBROKER -. "drained by" .-> KCONS
    KCONS  -. "writes" .-> CH_RAW

    CLEAN -. "reads / writes" .-> CH_RAW
    EMB   -. "reads cleaned rows" .-> CLEAN
    EMB   -. "writes vectors" .-> VEC
    TOPIC -. "reads cleaned rows" .-> CLEAN
    AGG   -. "reads rows + topics" .-> CLEAN
    AGG   -. "reads vectors" .-> VEC
    AGG   -. "writes aggregates" .-> CH

    STREAMLIT -. "uses" .-> QAPI
    QAPI      -. "reads" .-> CH

    classDef sub fill:#f7f2ff,stroke:#7b4fbf,stroke-dasharray: 5 4;
    class ING,PROC,STORE,SERVE sub;
```

## 3. Deployment Diagram (UML)

```mermaid
flowchart LR
    subgraph ENV ["Cloud / Local environment"]
        direction LR

        subgraph DOCKER ["Docker host (local or HWR VPS)"]
            direction TB
            ART_PRODS["«container»<br/>producer_*.py<br/>(RSS, BBC, NewsAPI, scrapers)"]
            ART_CONS["«container»<br/>consumer_to_clickhouse.py<br/>(dlt Kafka consumer)"]
            ART_PROC["«container»<br/>processing.py<br/>(embeddings + topic)"]
            ART_KAFKA["«container»<br/>kafka broker<br/>(port 9092)"]
            ART_CH["«container»<br/>clickhouse-server<br/>(raw + modelled)"]
            ART_STREAMLIT["«container»<br/>streamlit_app.py<br/>(port 8501)"]
        end

        subgraph EXTAPI ["External APIs"]
            direction TB
            ART_NEWS["«artifact»<br/>NewsAPI"]
            ART_GDELT["«artifact»<br/>GDELT 2.0 DOC API"]
            ART_RSS["«artifact»<br/>RSS / scraped outlets"]
        end

        subgraph CLIENT ["Analyst browser"]
            ART_BROWSER["«artifact»<br/>Chrome / Safari"]
        end
    end

    ART_PRODS -- "HTTPS:443" --> ART_NEWS
    ART_PRODS -- "HTTPS:443" --> ART_GDELT
    ART_PRODS -- "HTTPS:443" --> ART_RSS
    ART_PRODS -- "kafka:9092 (produce)" --> ART_KAFKA
    ART_KAFKA -- "kafka:9092 (consume)" --> ART_CONS
    ART_CONS  -- "HTTP:8123 (native:9000)" --> ART_CH
    ART_PROC  -- "HTTP:8123 (native:9000)" --> ART_CH
    ART_STREAMLIT -- "HTTP:8123" --> ART_CH
    ART_BROWSER -- "HTTP:8501" --> ART_STREAMLIT

    classDef node fill:#dfe9ff,stroke:#1f4cbf,color:#0c1d3a;
    classDef art fill:#f0f4ff,stroke:#1f4cbf,color:#0c1d3a;
    class DOCKER,EXTAPI,CLIENT,ENV node;
    class ART_PRODS,ART_CONS,ART_PROC,ART_KAFKA,ART_CH,ART_STREAMLIT,ART_NEWS,ART_GDELT,ART_RSS,ART_BROWSER art;
```
