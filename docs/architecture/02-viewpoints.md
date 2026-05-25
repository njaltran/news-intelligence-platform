# 02. IEEE 1471 Viewpoints

> Stub. Jack to flesh out in Week 1.

Per IEEE 1471 (now ISO/IEC/IEEE 42010), an architecture is described through **viewpoints** that address specific **stakeholder concerns**.

## Viewpoint catalogue

| Viewpoint | Stakeholder concern | Artifact |
|-----------|---------------------|----------|
| Logical | What components exist and how do they interact? | Component diagram, sequence diagrams |
| Physical | Where does each component run, on what hardware? | Deployment diagram |
| Data | What is the canonical data model? Where is it modelled? | Schema docs, ER diagram |
| Deployment | How do we ship and update the system? | Docker compose, CI pipeline |
| Security | How are secrets, PII, and access managed? | Secrets handling, robots.txt compliance |

Each viewpoint becomes a subsection in the report.
