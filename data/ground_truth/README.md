# data/ground_truth

Hand-coded evaluation examples. Each row pairs the **same global event** with how each of the five target countries (DE, US, IT, MM, KZ) framed it.

Used to evaluate the topic clustering, sentiment classification, and narrative-divergence metric.

## Format

```yaml
event_id: 2026-04-15-cop-summit
event_description: COP30 summit closes with weakened fossil-fuel language
canonical_date: 2026-04-15
framings:
  - country: DE
    outlet: spiegel.de
    url: ...
    headline: ...
    framing_axis: [climate-urgency, geopolitics]
    sentiment: -0.3
    notes: ...
  - country: US
    ...
```

Target: 50 events by end of Week 3. Owned by Karina.
