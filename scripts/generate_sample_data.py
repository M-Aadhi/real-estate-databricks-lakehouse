"""Generate synthetic real estate source data (no real people, companies or properties).

Writes batch_001 (initial extract) to data/raw/*.csv and batch_002 (incremental extract)
to data/raw/incremental/batch_002/*.csv. Output is deterministic for a given --seed.

Known data-quality problems are injected on purpose so the Silver layer has something to
catch; they are listed in data/raw/DATA_ISSUES.md after generation.

    python scripts/generate_sample_data.py              # default seed 42
    python scripts/generate_sample_data.py --out /tmp/raw --seed 7
"""
from __future__ import annotations

import argparse
import csv
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from dateutil.relativedelta import relativedelta
from faker import Faker

AS_OF = date(2026, 9, 30)
HISTORY_START = date(2025, 4, 1)          # first rent charge month in the extract
MAINT_START = date(2025, 1, 1)
VALUATION_DATES = [date(2023, 12, 31), date(2024, 6, 30), date(2024, 12, 31), date(2025, 6, 30),
                   date(2025, 12, 31), date(2026, 6, 30)]
Q3_VALUATION = date(2026, 9, 30)

COLUMNS = {
    "properties": ["property_id", "property_name", "property_type", "address", "city", "state", "zip_code",
                   "acquisition_date", "acquisition_price", "status"],
    "units": ["unit_id", "property_id", "unit_number", "bedrooms", "bathrooms", "square_feet", "market_rent",
              "unit_status"],
    "tenants": ["tenant_id", "first_name", "last_name", "email", "phone", "created_date", "tenant_status"],
    "leases": ["lease_id", "tenant_id", "unit_id", "property_id", "lease_start_date", "lease_end_date",
               "monthly_rent", "security_deposit", "lease_status"],
    "payments": ["payment_id", "lease_id", "tenant_id", "property_id", "payment_date", "payment_due_date",
                 "amount_due", "amount_paid", "payment_status", "payment_method"],
    "valuations": ["valuation_id", "property_id", "valuation_date", "property_value", "valuation_method"],
    "maintenance": ["maintenance_id", "property_id", "unit_id", "request_date", "completion_date", "category",
                    "cost", "status"],
}

CITIES = [  # (city, state, zip prefix, rent factor, weight)
    ("Houston", "TX", "770", 1.00, 18), ("Dallas", "TX", "752", 1.05, 14), ("Austin", "TX", "787", 1.20, 12),
    ("San Antonio", "TX", "782", 0.90, 10), ("Fort Worth", "TX", "761", 0.95, 8), ("Plano", "TX", "750", 1.10, 6),
    ("Katy", "TX", "774", 0.98, 5), ("The Woodlands", "TX", "773", 1.08, 5), ("Frisco", "TX", "750", 1.12, 5),
    ("Round Rock", "TX", "786", 1.05, 4), ("Sugar Land", "TX", "774", 1.06, 4), ("Oklahoma City", "OK", "731", 0.82, 3),
    ("Phoenix", "AZ", "850", 1.02, 3), ("Atlanta", "GA", "303", 1.08, 3),
]
TYPE_MIX = [("Multifamily", 45), ("Office", 15), ("Retail", 15), ("Industrial", 15), ("Mixed Use", 10)]
NAME_SUFFIX = {
    "Multifamily": ["Apartments", "Residences", "Lofts", "Flats", "Village"],
    "Office": ["Office Park", "Plaza", "Tower", "Business Center"],
    "Retail": ["Shopping Center", "Marketplace", "Village Shops", "Square"],
    "Industrial": ["Logistics Center", "Distribution Hub", "Industrial Park", "Commerce Center"],
    "Mixed Use": ["District", "Station", "Crossing", "Commons"],
}
NAME_PREFIX = ["Cypress Creek", "Bayou Bend", "Lone Star", "Pecan Grove", "Live Oak", "Riverwalk", "Magnolia",
               "Bluebonnet", "Westpark", "Mesquite Ridge", "Cedar Hill", "Stonebridge", "Prairie View",
               "Lakeside", "Oak Hollow", "Redbud", "Brazos", "Trinity", "Sabine", "Colorado River", "Hill Country",
               "Galleria", "Memorial", "Heights", "Uptown", "Midtown", "Eastside", "Northpoint", "Southgate",
               "Willow Bend", "Sunset Ridge", "Canyon", "Granite", "Summit", "Harbor", "Meridian", "Copper",
               "Silverleaf", "Ironwood", "Juniper"]
MAINT_CATEGORIES = [  # (category, weight, min cost, max cost)
    ("HVAC", 18, 150, 4500), ("Plumbing", 20, 90, 2500), ("Electrical", 12, 120, 3000), ("Appliance", 12, 80, 1400),
    ("Roofing", 4, 800, 12000), ("Landscaping", 8, 150, 1800), ("Painting", 8, 200, 2500),
    ("Pest Control", 8, 75, 600), ("General", 10, 50, 900),
]


@dataclass
class World:
    properties: list[dict] = field(default_factory=list)
    units: list[dict] = field(default_factory=list)
    tenants: list[dict] = field(default_factory=list)
    leases: list[dict] = field(default_factory=list)
    payments: list[dict] = field(default_factory=list)
    valuations: list[dict] = field(default_factory=list)
    maintenance: list[dict] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)


def money(x: float) -> str:
    return f"{x:.2f}"


def month_starts(start: date, end: date):
    d = date(start.year, start.month, 1)
    while d <= end:
        yield d
        d += relativedelta(months=1)


class Generator:
    def __init__(self, seed: int):
        self.rng = random.Random(seed)
        self.fake = Faker("en_US")
        self.fake.seed_instance(seed)
        self.w = World()
        self.counters = defaultdict(int)
        self.payer_profile: dict[str, str] = {}

    def next_id(self, prefix: str, width: int) -> str:
        self.counters[prefix] += 1
        return f"{prefix}{self.counters[prefix]:0{width}d}"

    def pick_weighted(self, items):
        return self.rng.choices(items, weights=[i[-1] for i in items])[0]

    # ---------------- entities ----------------
    def make_properties(self, n: int = 100) -> None:
        types = [t for t, w in TYPE_MIX for _ in range(w)]
        self.rng.shuffle(types)
        used = set()
        for ptype in types[:n]:
            city, state, zip3, factor, _ = self.pick_weighted(CITIES)
            while True:
                name = f"{self.rng.choice(NAME_PREFIX)} {self.rng.choice(NAME_SUFFIX[ptype])}"
                if name not in used:
                    used.add(name)
                    break
            acq = date(self.rng.randint(2012, 2024), self.rng.randint(1, 12), self.rng.randint(1, 28))
            self.w.properties.append({
                "property_id": self.next_id("P", 4), "property_name": name, "property_type": ptype,
                "address": self.fake.street_address(), "city": city, "state": state,
                "zip_code": f"{zip3}{self.rng.randint(1, 99):02d}", "acquisition_date": acq.isoformat(),
                "acquisition_price": 0.0, "status": "Under Renovation" if self.rng.random() < 0.05 else "Active",
                "_factor": factor,
            })

    def make_units(self) -> None:
        for p in self.w.properties:
            ptype, f = p["property_type"], p["_factor"]
            count = {"Multifamily": (10, 22), "Office": (4, 9), "Retail": (3, 8), "Industrial": (2, 5),
                     "Mixed Use": (10, 18)}[ptype]
            n = self.rng.randint(*count)
            total_value = 0.0
            for i in range(n):
                residential = ptype == "Multifamily" or (ptype == "Mixed Use" and i >= 3)
                if residential:
                    beds = self.rng.choice([0, 1, 1, 1, 2, 2, 3])
                    baths = {0: 1.0, 1: 1.0, 2: self.rng.choice([1.0, 2.0]), 3: 2.0}[beds]
                    sqft = {0: 520, 1: 720, 2: 1020, 3: 1300}[beds] + self.rng.randint(-60, 120)
                    rent = ({0: 1050, 1: 1300, 2: 1700, 3: 2150}[beds] * f) * self.rng.uniform(0.92, 1.1)
                    number = f"{(i // 12) + 1}{(i % 12) + 1:02d}"
                else:
                    kind = "Retail" if ptype == "Mixed Use" else ptype
                    beds, baths = 0, float(self.rng.choice([1, 2]))
                    sqft = {"Office": self.rng.randint(1200, 8000), "Retail": self.rng.randint(1000, 5000),
                            "Industrial": self.rng.randint(10000, 60000)}[kind]
                    rate = {"Office": 28, "Retail": 24, "Industrial": 9}[kind] * f * self.rng.uniform(0.9, 1.1)
                    rent = sqft * rate / 12
                    number = f"Suite {100 + i}" if kind != "Industrial" else f"Bay {i + 1}"
                total_value += rent * 12
                self.w.units.append({
                    "unit_id": self.next_id("U", 5), "property_id": p["property_id"], "unit_number": number,
                    "bedrooms": beds, "bathrooms": f"{baths:.1f}", "square_feet": sqft, "market_rent": money(rent),
                    "unit_status": "", "_residential": residential, "_ptype": ptype,
                })
            cap_rate = {"Multifamily": 0.055, "Office": 0.075, "Retail": 0.07, "Industrial": 0.06,
                        "Mixed Use": 0.065}[ptype]
            p["acquisition_price"] = round(total_value * 0.55 / cap_rate * self.rng.uniform(0.8, 0.95), -3)

    def new_tenant(self, created: date, status: str) -> dict:
        first, last = self.fake.first_name(), self.fake.last_name()
        t = {"tenant_id": self.next_id("T", 5), "first_name": first, "last_name": last,
             "email": f"{first.lower()}.{last.lower()}{self.rng.randint(1, 99)}@example.com",
             "phone": f"{self.rng.choice(['713', '214', '512', '210', '817', '469'])}-555-{self.rng.randint(0, 9999):04d}",
             "created_date": created.isoformat(), "tenant_status": status}
        self.w.tenants.append(t)
        self.payer_profile[t["tenant_id"]] = self.rng.choices(["good", "late", "struggling"], [80, 13, 7])[0]
        return t

    def add_lease(self, unit: dict, tenant: dict, start: date, end: date, status: str) -> dict:
        rent = float(unit["market_rent"]) * self.rng.uniform(0.94, 1.02)
        lease = {"lease_id": self.next_id("L", 6), "tenant_id": tenant["tenant_id"], "unit_id": unit["unit_id"],
                 "property_id": unit["property_id"], "lease_start_date": start.isoformat(),
                 "lease_end_date": end.isoformat(), "monthly_rent": money(rent),
                 "security_deposit": money(rent if unit["_residential"] else rent * 2), "lease_status": status}
        self.w.leases.append(lease)
        return lease

    def make_leases(self) -> None:
        reno = {p["property_id"] for p in self.w.properties if p["status"] == "Under Renovation"}
        for u in self.w.units:
            occ_p = {"Multifamily": 0.93, "Office": 0.85, "Retail": 0.88, "Industrial": 0.94, "Mixed Use": 0.9}[u["_ptype"]]
            if u["property_id"] in reno:
                occ_p = 0.6
            occupied = self.rng.random() < occ_p
            term = 12 if u["_residential"] else self.rng.choice([36, 48, 60])
            cur_start = None
            if occupied:
                months_in = self.rng.randint(0, term - 1)
                cur_start = date(AS_OF.year, AS_OF.month, 1) - relativedelta(months=months_in)
                cur_end = cur_start + relativedelta(months=term) - timedelta(days=1)
                tenant = self.new_tenant(cur_start - timedelta(days=self.rng.randint(10, 40)), "Active")
                self.add_lease(u, tenant, cur_start, cur_end, "Active")
                u["unit_status"] = "Occupied"
            else:
                u["unit_status"] = self.rng.choice(["Vacant", "Vacant", "Notice"])  # source flag is noisy
            # previous lease (history) for most units
            if self.rng.random() < (0.75 if u["_residential"] else 0.5):
                prev_end = (cur_start - timedelta(days=1)) if cur_start else \
                    AS_OF - relativedelta(months=self.rng.randint(1, 5))
                if not cur_start:
                    prev_end = date(prev_end.year, prev_end.month, 1) - timedelta(days=1)
                prev_start = prev_end + timedelta(days=1) - relativedelta(months=term)
                renewal = cur_start is not None and self.rng.random() < 0.3
                tenant = (next(t for t in reversed(self.w.tenants)) if renewal else
                          self.new_tenant(prev_start - timedelta(days=self.rng.randint(10, 40)), "Former"))
                status = "Terminated" if self.rng.random() < 0.04 else "Expired"
                self.add_lease(u, tenant, prev_start, prev_end, status)
        # prospects without leases (realistic CRM noise)
        for _ in range(15):
            self.new_tenant(AS_OF - timedelta(days=self.rng.randint(5, 60)), "Prospect")

    def make_payments(self) -> None:
        methods = [("ACH", 55), ("Card", 20), ("Check", 20), ("Cash", 5)]
        for lease in self.w.leases:
            start, end = date.fromisoformat(lease["lease_start_date"]), date.fromisoformat(lease["lease_end_date"])
            if lease["lease_status"] == "Terminated":
                end = min(end, start + relativedelta(months=self.rng.randint(2, 6)))
            profile = self.payer_profile[lease["tenant_id"]]
            due_months = [m for m in month_starts(max(start, HISTORY_START), min(end, AS_OF)) if m <= AS_OF]
            for idx, m in enumerate(due_months):
                due = max(m, start)
                rent = float(lease["monthly_rent"])
                months_from_end = len(due_months) - 1 - idx
                paid, pay_date, status = rent, due + timedelta(days=self.rng.randint(0, 4)), "Paid"
                if profile == "late" and self.rng.random() < 0.4:
                    pay_date, status = due + timedelta(days=self.rng.randint(6, 20)), "Late"
                if profile == "struggling":
                    if months_from_end <= self.rng.randint(1, 4):
                        if self.rng.random() < 0.5:
                            paid, pay_date, status = 0.0, None, "Unpaid"
                        else:
                            paid, status = round(rent * self.rng.uniform(0.3, 0.8), 2), "Partial"
                            pay_date = due + timedelta(days=self.rng.randint(3, 15))
                    elif self.rng.random() < 0.5:
                        pay_date, status = due + timedelta(days=self.rng.randint(6, 25)), "Late"
                if due.year == AS_OF.year and due.month == AS_OF.month and profile != "good" \
                        and self.rng.random() < 0.5:
                    paid, pay_date, status = 0.0, None, "Unpaid"   # September still open at extract time
                if pay_date and pay_date > AS_OF:
                    paid, pay_date, status = 0.0, None, "Unpaid"
                self.w.payments.append({
                    "payment_id": self.next_id("PMT", 7), "lease_id": lease["lease_id"],
                    "tenant_id": lease["tenant_id"], "property_id": lease["property_id"],
                    "payment_date": pay_date.isoformat() if pay_date else "", "payment_due_date": due.isoformat(),
                    "amount_due": money(rent), "amount_paid": money(paid), "payment_status": status,
                    "payment_method": self.pick_weighted(methods)[0] if paid > 0 else ""})

    def make_valuations(self) -> None:
        methods = ["Income Approach", "Sales Comparison", "Broker Opinion of Value"]
        for p in self.w.properties:
            drift = {"Industrial": 0.06, "Multifamily": 0.04, "Retail": 0.02, "Mixed Use": 0.03,
                     "Office": -0.02}[p["property_type"]] + self.rng.gauss(0, 0.02)
            acq = date.fromisoformat(p["acquisition_date"])
            for d in VALUATION_DATES:
                if d < acq:
                    continue
                years = (d - acq).days / 365.25
                value = p["acquisition_price"] * (1 + drift) ** years * self.rng.uniform(0.98, 1.02)
                self.w.valuations.append({"valuation_id": self.next_id("V", 6), "property_id": p["property_id"],
                                          "valuation_date": d.isoformat(), "property_value": money(round(value, -3)),
                                          "valuation_method": self.rng.choice(methods)})
            p["_drift"] = drift

    def make_maintenance(self) -> None:
        units_by_prop = defaultdict(list)
        for u in self.w.units:
            units_by_prop[u["property_id"]].append(u["unit_id"])
        days = (AS_OF - MAINT_START).days
        for p in self.w.properties:
            n = int(len(units_by_prop[p["property_id"]]) * self.rng.uniform(1.0, 1.9))
            for _ in range(n):
                cat, _, lo, hi = self.pick_weighted(MAINT_CATEGORIES)
                req = MAINT_START + timedelta(days=self.rng.randint(0, days))
                age = (AS_OF - req).days
                if age > 30:
                    status = self.rng.choices(["Completed", "Cancelled", "In Progress"], [90, 5, 5])[0]
                else:
                    status = self.rng.choices(["Open", "In Progress", "Completed"], [35, 30, 35])[0]
                done = req + timedelta(days=self.rng.randint(0, min(20, age))) if status == "Completed" else None
                common = cat in ("Landscaping", "Roofing") or self.rng.random() < 0.15
                self.w.maintenance.append({
                    "maintenance_id": self.next_id("M", 6), "property_id": p["property_id"],
                    "unit_id": "" if common else self.rng.choice(units_by_prop[p["property_id"]]),
                    "request_date": req.isoformat(), "completion_date": done.isoformat() if done else "",
                    "category": cat, "cost": money(self.rng.uniform(lo, hi) if status != "Cancelled" else 0),
                    "status": status})

    # ---------------- injected data issues (batch_001) ----------------
    def inject_issues(self) -> list[list[str]]:
        w, rng, log = self.w, self.rng, self.w.issues
        # properties: exact duplicates re-sent by the source
        for p in rng.sample(w.properties, 2):
            w.properties.append(dict(p))
            log.append(f"properties: exact duplicate of {p['property_id']}")
        # units: orphan property, unparseable square feet
        for u in rng.sample(w.units, 2):
            w.units.append({**u, "unit_id": self.next_id("U", 5), "property_id": "P9999"})
            log.append(f"units: {w.units[-1]['unit_id']} references unknown property P9999")
        bad = rng.choice(w.units)
        w.units.append({**bad, "unit_id": self.next_id("U", 5), "square_feet": "approx 900"})
        log.append(f"units: {w.units[-1]['unit_id']} has non-numeric square_feet")
        # tenants: bad emails, exact duplicates
        for t in rng.sample([t for t in w.tenants if t["tenant_status"] == "Prospect"], 2):
            t["email"] = t["email"].replace("@", "[at]")
            log.append(f"tenants: {t['tenant_id']} has invalid email")
        for t in rng.sample(w.tenants, 3):
            w.tenants.append(dict(t))
            log.append(f"tenants: exact duplicate of {t['tenant_id']}")
        # leases: only corrupt old leases without charges in the extract window, so the
        # issue is visible in Silver without cascading into thousands of payment orphans
        charged = {p["lease_id"] for p in w.payments}
        old = [l for l in w.leases if l["lease_id"] not in charged]
        picks = rng.sample(old, 10)
        for l in picks[:4]:
            l["lease_end_date"] = (date.fromisoformat(l["lease_start_date"]) - timedelta(days=30)).isoformat()
            log.append(f"leases: {l['lease_id']} ends before it starts")
        for l in picks[4:6]:
            l["monthly_rent"] = "-" + l["monthly_rent"]
            log.append(f"leases: {l['lease_id']} has negative rent")
        for l in picks[6:8]:
            l["lease_start_date"] = "2024-02-30"
            log.append(f"leases: {l['lease_id']} has an invalid date (2024-02-30)")
        for l in picks[8:10]:
            w.leases.append({**l, "monthly_rent": money(float(l["monthly_rent"]) + 125)})
            log.append(f"leases: {l['lease_id']} sent twice with different rent (conflicting duplicate)")
        w.leases.append({**rng.choice(old[20:]), "lease_id": self.next_id("L", 6), "tenant_id": "T99999"})
        log.append(f"leases: {w.leases[-1]['lease_id']} references unknown tenant T99999")
        for l in rng.sample([l for l in w.leases if l["lease_status"] == "Active"], 2):
            w.leases.append(dict(l))
            log.append(f"leases: exact duplicate of {l['lease_id']}")
        # payments: re-sent duplicates, negative amounts, orphans, bad dates
        for p in rng.sample(w.payments, 25):
            w.payments.append(dict(p))
        log.append("payments: 25 exact duplicate rows (source re-sent)")
        for p in rng.sample(w.payments, 3):
            w.payments.append({**p, "payment_id": self.next_id("PMT", 7), "amount_paid": "-50.00"})
            log.append(f"payments: {w.payments[-1]['payment_id']} has negative amount_paid")
        for p in rng.sample(w.payments, 4):
            w.payments.append({**p, "payment_id": self.next_id("PMT", 7), "lease_id": "L999999"})
            log.append(f"payments: {w.payments[-1]['payment_id']} references unknown lease L999999")
        for p in rng.sample(w.payments, 2):
            w.payments.append({**p, "payment_id": self.next_id("PMT", 7), "payment_due_date": "09/01/2026"})
            log.append(f"payments: {w.payments[-1]['payment_id']} has a date in the wrong format")
        malformed = [f"{self.next_id('PMT', 7)},L000001,T00001,P0001,2026-09-02,2026-09-01,1500.00,1500.00,Paid,ACH,EXTRA,FIELDS",
                     f"{self.next_id('PMT', 7)},\"L000002,T00002"]
        log.append("payments: 2 malformed CSV rows (wrong number of fields) -> bronze_rejects")
        # valuations: non-positive values, orphan
        for v in rng.sample(w.valuations, 2):
            w.valuations.append({**v, "valuation_id": self.next_id("V", 6), "property_value": "0.00"})
            log.append(f"valuations: {w.valuations[-1]['valuation_id']} has property_value 0")
        w.valuations.append({**w.valuations[0], "valuation_id": self.next_id("V", 6), "property_id": "P9999"})
        log.append(f"valuations: {w.valuations[-1]['valuation_id']} references unknown property")
        # maintenance: negative cost, completion before request, unknown unit
        for m in rng.sample(w.maintenance, 2):
            w.maintenance.append({**m, "maintenance_id": self.next_id("M", 6), "cost": "-120.00"})
            log.append(f"maintenance: {w.maintenance[-1]['maintenance_id']} has negative cost")
        for m in rng.sample([m for m in w.maintenance if m["completion_date"]], 2):
            req = date.fromisoformat(m["request_date"])
            w.maintenance.append({**m, "maintenance_id": self.next_id("M", 6),
                                  "completion_date": (req - timedelta(days=3)).isoformat()})
            log.append(f"maintenance: {w.maintenance[-1]['maintenance_id']} completed before requested")
        for m in rng.sample(w.maintenance, 2):
            w.maintenance.append({**m, "maintenance_id": self.next_id("M", 6), "unit_id": "U99999"})
            log.append(f"maintenance: {w.maintenance[-1]['maintenance_id']} references unknown unit")
        return [malformed]

    # ---------------- batch_002 (incremental) ----------------
    def make_batch_002(self) -> tuple[dict[str, list[dict]], list[str]]:
        rng, w, notes = self.rng, self.w, []
        tenants, leases, payments, valuations = [], [], [], []
        originals = [t for t in w.tenants if t["tenant_status"] in ("Active", "Former")]
        seen = set()
        for t in rng.sample(originals, 15):
            if t["tenant_id"] in seen:
                continue
            seen.add(t["tenant_id"])
            tenants.append({**t, "email": f"{t['first_name'].lower()}.{t['last_name'].lower()}.new@example.com",
                            "phone": f"832-555-{rng.randint(0, 9999):04d}"})
        notes.append(f"tenants: {len(tenants)} existing tenants changed email/phone (SCD2 new versions)")
        for t in rng.sample([t for t in originals if t["tenant_id"] not in seen], 3):
            tenants.append(dict(t))
        notes.append("tenants: 3 unchanged tenants re-sent (must not create new versions)")

        active_units = {l["unit_id"] for l in w.leases if l["lease_status"] == "Active"}
        units = {u["unit_id"]: u for u in w.units}
        vacant = [u for u in w.units if u["unit_id"] not in active_units and u["_residential"]
                  and u["property_id"] != "P9999" and u["square_feet"] != "approx 900"]
        new_leases = []
        for u in rng.sample(vacant, 20):
            start = date(2026, 9, rng.randint(15, 28))
            before = len(w.tenants)
            tenant = self.new_tenant(start - timedelta(days=rng.randint(5, 20)), "Active")
            w.tenants.pop(before)   # keep the new tenant out of batch_001
            tenants.append(tenant)
            rent = float(u["market_rent"]) * rng.uniform(0.97, 1.03)
            lease = {"lease_id": self.next_id("L", 6), "tenant_id": tenant["tenant_id"], "unit_id": u["unit_id"],
                     "property_id": u["property_id"], "lease_start_date": start.isoformat(),
                     "lease_end_date": (start + relativedelta(months=12) - timedelta(days=1)).isoformat(),
                     "monthly_rent": money(rent), "security_deposit": money(rent), "lease_status": "Active"}
            leases.append(lease)
            new_leases.append(lease)
        notes.append("leases: 20 new move-ins (15-28 Sep 2026) reported late -> occupancy rises")
        bad_email = dict(tenants[-1])
        bad_email.update(tenant_id=self.next_id("T", 5), email="no-email-provided")
        tenants.append(bad_email)
        notes.append(f"tenants: {bad_email['tenant_id']} has an invalid email -> quarantine")

        expiring = [l for l in w.leases if l["lease_status"] == "Active" and l["lease_start_date"][0] == "2"
                    and AS_OF <= date.fromisoformat(l["lease_end_date"]) <= AS_OF + timedelta(days=90)
                    and units[l["unit_id"]]["_residential"]]
        renewals = rng.sample(expiring, min(10, len(expiring)))
        for l in renewals:
            end = date.fromisoformat(l["lease_end_date"]) + relativedelta(months=12)
            leases.append({**l, "lease_end_date": end.isoformat(),
                           "monthly_rent": money(float(l["monthly_rent"]) * 1.03)})
        notes.append(f"leases: {len(renewals)} renewals (same lease_id, +12 months, +3% rent) -> MERGE updates")
        for l in rng.sample([l for l in w.leases if l["lease_status"] == "Active" and l not in renewals], 5):
            leases.append(dict(l))
        notes.append("leases: 5 unchanged leases re-sent (no-op)")
        bad = dict(new_leases[0])
        bad.update(lease_id=self.next_id("L", 6), unit_id="U99999")
        leases.append(bad)
        notes.append(f"leases: {bad['lease_id']} references unknown unit -> quarantine")

        open_sept = [p for p in w.payments if p["payment_due_date"].startswith("2026-09")
                     and float(p["amount_paid"]) < float(p["amount_due"]) and p["lease_id"] != "L999999"
                     and not p["amount_paid"].startswith("-")]
        late = rng.sample(open_sept, min(30, len(open_sept)))
        for p in late:
            payments.append({**p, "amount_paid": p["amount_due"], "payment_status": "Late",
                             "payment_date": date(2026, 9, rng.randint(8, 29)).isoformat(),
                             "payment_method": rng.choice(["ACH", "Card", "Check"])})
        notes.append(f"payments: {len(late)} open September charges now paid (late arrivals) -> MERGE updates")
        for l in new_leases:
            start = date.fromisoformat(l["lease_start_date"])
            paid = rng.random() < 0.7
            payments.append({"payment_id": self.next_id("PMT", 7), "lease_id": l["lease_id"],
                             "tenant_id": l["tenant_id"], "property_id": l["property_id"],
                             "payment_date": (start + timedelta(days=1)).isoformat() if paid else "",
                             "payment_due_date": start.isoformat(), "amount_due": l["monthly_rent"],
                             "amount_paid": l["monthly_rent"] if paid else "0.00",
                             "payment_status": "Paid" if paid else "Unpaid", "payment_method": "ACH" if paid else ""})
        notes.append("payments: first-month charges for the 20 new leases")
        for p in rng.sample([p for p in w.payments if p not in late and p["lease_id"] != "L999999"
                             and not p["amount_paid"].startswith("-") and "/" not in p["payment_due_date"]], 15):
            payments.append(dict(p))
        notes.append("payments: 15 unchanged payments re-sent (must not duplicate)")
        payments.append({**payments[0], "payment_id": self.next_id("PMT", 7), "amount_paid": "-10.00"})
        notes.append(f"payments: {payments[-1]['payment_id']} negative amount -> quarantine")

        unique_props = {p["property_id"]: p for p in w.properties}.values()   # skip re-sent duplicates
        for p in unique_props:
            last = max((v for v in w.valuations if v["property_id"] == p["property_id"]
                        and float(v["property_value"]) > 0), key=lambda v: v["valuation_date"], default=None)
            if not last:
                continue
            value = float(last["property_value"]) * (1 + p["_drift"]) ** 0.25 * rng.uniform(0.99, 1.01)
            valuations.append({"valuation_id": self.next_id("V", 6), "property_id": p["property_id"],
                               "valuation_date": Q3_VALUATION.isoformat(), "property_value": money(round(value, -3)),
                               "valuation_method": "Income Approach"})
        notes.append(f"valuations: {len(valuations)} Q3-2026 valuations (new rows, value history extends)")
        return {"tenants": tenants, "leases": leases, "payments": payments, "valuations": valuations}, notes


def write_csv(path: Path, columns: list[str], rows: list[dict], raw_lines: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        for line in raw_lines or []:
            fh.write(line + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "data" / "raw"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    out = Path(args.out)

    g = Generator(args.seed)
    g.make_properties()
    g.make_units()
    g.make_leases()
    g.make_payments()
    g.make_valuations()
    g.make_maintenance()
    [malformed_payments] = g.inject_issues()
    batch2, notes2 = g.make_batch_002()

    w = g.w
    for name in COLUMNS:
        raw = malformed_payments if name == "payments" else None
        write_csv(out / f"{name}.csv", COLUMNS[name], getattr(w, name), raw)
    for name, rows in batch2.items():
        write_csv(out / "incremental" / "batch_002" / f"{name}.csv", COLUMNS[name], rows)

    report = ["# Injected data issues", "",
              "Generated by `scripts/generate_sample_data.py` (synthetic data only). These problems are",
              "intentional so the Silver layer can demonstrate validation, quarantine and deduplication.", "",
              "## batch_001 (data/raw)", ""] + [f"- {i}" for i in w.issues] + \
             ["", "## batch_002 (data/raw/incremental/batch_002)", ""] + [f"- {n}" for n in notes2] + [""]
    (out / "DATA_ISSUES.md").write_text("\n".join(report), encoding="utf-8")

    print("batch_001:", {n: len(getattr(w, n)) for n in COLUMNS})
    print("batch_002:", {n: len(r) for n, r in batch2.items()})


if __name__ == "__main__":
    main()
