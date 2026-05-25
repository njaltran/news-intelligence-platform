# Must-Read Papers — Enterprise Architectures for Big Data

A curated reading list for this course and the **OpenTelemetry streaming** project. The course `resources/` folder already covers Banko & Brill (2001), Halevy/Norvig/Pereira (2009), and Watson (2014); the papers below extend those foundations into systems, streaming, and observability — the stack the OTel project actually has to build on.

Papers marked **[OTel]** are the ones most directly relevant to the class project.

---

## 1 · Foundations of Big Data systems

### Dean & Ghemawat — *MapReduce: Simplified Data Processing on Large Clusters* (OSDI 2004)
The paper that made "big data" a programming model instead of a buzzword. Read it to understand why every later system (Spark, Flink, Beam) either embraces or rebels against map + shuffle + reduce.
- https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/

### Ghemawat, Gobioff & Leung — *The Google File System* (SOSP 2003)
Commodity hardware, append-heavy workloads, relaxed consistency. The blueprint for HDFS and every object store that followed.
- https://research.google/pubs/the-google-file-system/

### Chang et al. — *Bigtable: A Distributed Storage System for Structured Data* (OSDI 2006)
Sparse, distributed, sorted map. Triggered the NoSQL movement (HBase, Cassandra, Accumulo).
- https://research.google/pubs/bigtable-a-distributed-storage-system-for-structured-data/

### DeCandia et al. — *Dynamo: Amazon's Highly Available Key-value Store* (SOSP 2007)
Where "eventual consistency," consistent hashing, and vector clocks entered mainstream practice. Pair with Bigtable to see the CP vs AP split in the wild.
- https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf

### Gilbert & Lynch — *Brewer's Conjecture and the Feasibility of Consistent, Available, Partition-tolerant Web Services* (SIGACT 2002)
The formal proof of CAP. Short, readable, and the theoretical backbone for every trade-off decision in the papers above.
- https://groups.csail.mit.edu/tds/papers/Gilbert/Brewer2.pdf

---

## 2 · The batch → stream transition

### Zaharia et al. — *Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing* (NSDI 2012) — *Best Paper*
RDDs, lineage-based recovery, and the reason Spark displaced Hadoop MapReduce for iterative and interactive workloads.
- https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia

### Kreps — *The Log: What Every Software Engineer Should Know About Real-Time Data's Unifying Abstraction* (LinkedIn Engineering, 2013) **[OTel]**
Not a peer-reviewed paper, but the most-cited engineering essay on why the append-only log is the substrate for Kafka, stream processing, and event-driven architecture. Required reading before touching Kafka.
- https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying

### Kreps — *Questioning the Lambda Architecture* (O'Reilly Radar, 2014) **[OTel]**
The "Kappa" argument: one streaming pipeline, reprocess by replaying the log. Directly shapes OTel collector topologies — batch layer or not?
- https://www.oreilly.com/radar/questioning-the-lambda-architecture/

### Marz — *How to Beat the CAP Theorem* (2011)
The original Lambda-architecture essay. Even if Kappa wins on simplicity, you need Marz's framing to understand the debate.
- http://nathanmarz.com/blog/how-to-beat-the-cap-theorem.html

---

## 3 · Modern stream processing **[OTel core]**

### Akidau et al. — *MillWheel: Fault-Tolerant Stream Processing at Internet Scale* (VLDB 2013)
Exactly-once semantics, low-watermarks, out-of-order processing. Google's proof-of-concept that production streaming at scale is tractable.
- https://research.google/pubs/millwheel-fault-tolerant-stream-processing-at-internet-scale/

### Akidau et al. — *The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing* (VLDB 2015)
The unified model behind Apache Beam. Event-time vs processing-time, windowing, triggers, watermarks — the vocabulary every streaming system now uses.
- https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf

### Carbone et al. — *Apache Flink™: Stream and Batch Processing in a Single Engine* (IEEE Data Eng. Bulletin, 2015)
How Flink realises the Dataflow model with distributed snapshots (Chandy–Lamport) and pipelined shuffles. Reference point for anything doing stateful stream processing.
- https://asterios.katsifodimos.com/assets/publications/flink-deb.pdf

### Kreps, Narkhede & Rao — *Kafka: A Distributed Messaging System for Log Processing* (NetDB 2011)
The original Kafka paper. Short; enough to understand the partition + offset + consumer-group model that every OTel exporter eventually talks to.
- https://notes.stephenholiday.com/Kafka.pdf

---

## 4 · Observability & distributed tracing **[OTel core]**

### Sigelman et al. — *Dapper, a Large-Scale Distributed Systems Tracing Infrastructure* (Google Technical Report, 2010)
The paper OpenTelemetry descends from. Spans, trace context propagation, sampling strategy, application-level transparency — every design choice in OTel traces back here.
- https://research.google/pubs/dapper-a-large-scale-distributed-systems-tracing-infrastructure/

### Beyer, Jones, Petoff & Murphy — *Site Reliability Engineering* (O'Reilly, 2016) — Ch. 6 "Monitoring Distributed Systems"
Google's four golden signals (latency, traffic, errors, saturation). Free online. The framing most OTel dashboards are still organised around.
- https://sre.google/sre-book/monitoring-distributed-systems/

### Majors, Fong-Jones & Miranda — *Observability Engineering* (O'Reilly, 2022) — Ch. 1–3
Why "logs + metrics + traces" became one problem (high-cardinality events) and how OTel is the answer. Book, not a paper, but the ideas are the state of the art.
- https://www.oreilly.com/library/view/observability-engineering/9781492076438/

---

## 5 · Reading order suggestions

- **Before the oral exam:** re-read the three course papers in `resources/` → CAP → The Log → Dataflow Model. That's the intellectual arc of the course in ~4 hours.
- **Before writing OTel code:** Dapper → Kafka → The Log → Questioning the Lambda Architecture → MillWheel. You'll design a collector topology with far fewer false starts.
- **For the report's "related work" section:** MapReduce, GFS, Bigtable, RDDs, Dataflow, Dapper — the six citations that anchor any Big-Data-architecture report.
