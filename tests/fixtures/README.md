# Test fixtures

Tiny, hand-checked datasets for the integration and data-quality tests (the large generated
dataset is NOT used in tests). Expected results are derived by hand in
`tests/integration/test_pipeline.py`.

batch_001 contains, on purpose:
- an exact duplicate property (P2), lease (L3) and payment (PMT2)
- a unit with an unknown property (U9), a tenant with an invalid email (T4)
- leases that end before they start (L4), reference an unknown tenant (L5) or a unit of
  another property (L6)
- payments with an unknown lease (PMT7), a negative amount (PMT8) and a malformed CSV row (PMT9)
- a zero valuation (V5) and a maintenance job completed before it was requested (M6)

batch_002 is the incremental load: tenant T2 changes email (SCD2), new tenant T5 + lease L7,
lease L2 is renewed, payment PMT4 arrives late, PMT2/L3/T1 are re-sent unchanged and PMT11 is invalid.
