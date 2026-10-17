"""Source entity definitions: one place for column types, keys and documentation.

Bronze reads every column as STRING (source values preserved). Silver casts with
`spark_type`. scripts/generate_data_dictionary.py turns this registry into
docs/data_dictionary.md, so code and documentation cannot drift apart.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Column:
    name: str
    spark_type: str          # Spark SQL type used in Silver
    nullable: bool
    description: str
    example: str
    rule: str = ""           # business rule enforced in Silver ("" = none)


@dataclass(frozen=True)
class Entity:
    name: str
    primary_key: str
    description: str
    columns: tuple[Column, ...]
    foreign_keys: dict[str, str] = field(default_factory=dict)   # column -> parent entity

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)


MONEY = "decimal(14,2)"

PROPERTIES = Entity(
    "properties", "property_id", "Real estate assets owned or managed by the company.",
    (
        Column("property_id", "string", False, "Unique property identifier", "P0001", "Primary key; unique"),
        Column("property_name", "string", False, "Marketing name of the property", "Cypress Creek Apartments"),
        Column("property_type", "string", False, "Asset class", "Multifamily",
               "One of Multifamily, Office, Retail, Industrial, Mixed Use"),
        Column("address", "string", True, "Street address", "1200 Westheimer Rd"),
        Column("city", "string", False, "City", "Houston"),
        Column("state", "string", False, "US state code", "TX", "Two-letter code, stored upper case"),
        Column("zip_code", "string", True, "ZIP code (kept as text to preserve leading zeros)", "77006"),
        Column("acquisition_date", "date", False, "Date the asset was acquired", "2019-06-14", "Valid date"),
        Column("acquisition_price", MONEY, False, "Purchase price in USD", "18500000.00", "> 0"),
        Column("status", "string", False, "Operational status", "Active", "Active, Under Renovation or Disposed"),
    ),
)

UNITS = Entity(
    "units", "unit_id", "Leasable spaces (apartments, suites, bays) inside a property.",
    (
        Column("unit_id", "string", False, "Unique unit identifier", "U00001", "Primary key; unique"),
        Column("property_id", "string", False, "Property the unit belongs to", "P0001",
               "Must exist in properties"),
        Column("unit_number", "string", False, "Unit label within the property", "204"),
        Column("bedrooms", "int", True, "Bedrooms (0 for commercial space)", "2", ">= 0"),
        Column("bathrooms", "decimal(3,1)", True, "Bathrooms", "2.0", ">= 0"),
        Column("square_feet", "int", False, "Rentable square feet", "950", "> 0"),
        Column("market_rent", MONEY, False, "Current asking rent per month (USD)", "1650.00", ">= 0"),
        Column("unit_status", "string", True, "Status reported by the property system", "Occupied",
               "Informational only; occupancy is derived from leases"),
    ),
    {"property_id": "properties"},
)

TENANTS = Entity(
    "tenants", "tenant_id", "Residents and commercial tenant contacts (synthetic). SCD Type 2 in Silver.",
    (
        Column("tenant_id", "string", False, "Unique tenant identifier", "T00001", "Primary key; unique per version"),
        Column("first_name", "string", False, "First name (fake)", "Maria"),
        Column("last_name", "string", False, "Last name (fake)", "Lopez"),
        Column("email", "string", True, "Email (fake, example.com domains)", "maria.lopez@example.com",
               "Lower case; must contain '@' when present"),
        Column("phone", "string", True, "Phone (fake)", "713-555-0142"),
        Column("created_date", "date", False, "Date the tenant record was created", "2024-03-02", "Valid date"),
        Column("tenant_status", "string", False, "Tenant status", "Active", "Active, Former or Prospect"),
    ),
)

LEASES = Entity(
    "leases", "lease_id", "Lease contracts between tenants and units.",
    (
        Column("lease_id", "string", False, "Unique lease identifier", "L000001", "Primary key; unique"),
        Column("tenant_id", "string", False, "Tenant on the lease", "T00001", "Must exist in tenants"),
        Column("unit_id", "string", False, "Leased unit", "U00001", "Must exist in units"),
        Column("property_id", "string", False, "Property of the leased unit", "P0001",
               "Must exist in properties and match the unit's property"),
        Column("lease_start_date", "date", False, "First day of the lease", "2025-07-01", "Valid date"),
        Column("lease_end_date", "date", False, "Last day of the lease", "2026-06-30", ">= lease_start_date"),
        Column("monthly_rent", MONEY, False, "Contracted rent per month (USD)", "1625.00", ">= 0"),
        Column("security_deposit", MONEY, True, "Deposit held (USD)", "1625.00", ">= 0"),
        Column("lease_status", "string", False, "Status in the leasing system", "Active",
               "Active, Expired, Terminated or Future"),
    ),
    {"tenant_id": "tenants", "unit_id": "units", "property_id": "properties"},
)

PAYMENTS = Entity(
    "payments", "payment_id", "Monthly rent charges and the amount collected against each.",
    (
        Column("payment_id", "string", False, "Unique charge/payment identifier", "PMT0000001", "Primary key; unique"),
        Column("lease_id", "string", False, "Lease being billed", "L000001", "Must exist in leases"),
        Column("tenant_id", "string", False, "Tenant billed", "T00001", "Must exist in tenants"),
        Column("property_id", "string", False, "Property", "P0001", "Must exist in properties"),
        Column("payment_date", "date", True, "Date money was received (null if unpaid)", "2026-09-03", "Valid date"),
        Column("payment_due_date", "date", False, "Due date of the charge", "2026-09-01", "Valid date"),
        Column("amount_due", MONEY, False, "Amount billed (USD)", "1625.00", ">= 0"),
        Column("amount_paid", MONEY, False, "Amount received (USD)", "1625.00", ">= 0"),
        Column("payment_status", "string", True, "Status reported by the source system", "Paid",
               "Kept as reported; Silver adds derived_payment_status"),
        Column("payment_method", "string", True, "ACH, Card, Check or Cash", "ACH"),
    ),
    {"lease_id": "leases", "tenant_id": "tenants", "property_id": "properties"},
)

VALUATIONS = Entity(
    "valuations", "valuation_id", "Appraisals / valuations of each property over time.",
    (
        Column("valuation_id", "string", False, "Unique valuation identifier", "V000001", "Primary key; unique"),
        Column("property_id", "string", False, "Valued property", "P0001", "Must exist in properties"),
        Column("valuation_date", "date", False, "Effective date of the valuation", "2026-06-30", "Valid date"),
        Column("property_value", MONEY, False, "Estimated market value (USD)", "21400000.00", "> 0"),
        Column("valuation_method", "string", True, "Method used", "Income Approach"),
    ),
    {"property_id": "properties"},
)

MAINTENANCE = Entity(
    "maintenance", "maintenance_id", "Work orders and their cost.",
    (
        Column("maintenance_id", "string", False, "Unique work order identifier", "M000001", "Primary key; unique"),
        Column("property_id", "string", False, "Property", "P0001", "Must exist in properties"),
        Column("unit_id", "string", True, "Unit (null for common areas)", "U00001", "Must exist in units when present"),
        Column("request_date", "date", False, "Date the request was opened", "2026-08-12", "Valid date"),
        Column("completion_date", "date", True, "Date the work was completed", "2026-08-15", ">= request_date"),
        Column("category", "string", False, "Work category", "HVAC"),
        Column("cost", MONEY, False, "Cost of the work (USD)", "480.00", ">= 0"),
        Column("status", "string", False, "Work order status", "Completed", "Open, In Progress, Completed or Cancelled"),
    ),
    {"property_id": "properties", "unit_id": "units"},
)

ENTITIES: dict[str, Entity] = {e.name: e for e in
                               (PROPERTIES, UNITS, TENANTS, LEASES, PAYMENTS, VALUATIONS, MAINTENANCE)}

# Metadata columns added by the Bronze layer.
BRONZE_METADATA = ("ingestion_timestamp", "source_file", "batch_id")
