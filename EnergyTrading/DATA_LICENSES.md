# Energy Trading Prices — Data Licenses and Attribution

This file is part of the published dataset and must be redistributed alongside
the data. It records where every series came from, the license that applies, and
the changes this pipeline makes.

**Dataset license:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

The dataset as a whole is published under CC BY 4.0 because that is the most
restrictive upstream license. The EIA portion is public domain, but combining it
with CC BY 4.0 data means the combined work is offered under CC BY 4.0.

**Audit date:** 2026-11-08. This is an attribution record, not legal advice.
Always re-check the linked terms before relying on them commercially.

---

## 1. U.S. Energy Information Administration (EIA)

| Field | Value |
|---|---|
| Data | Daily spot prices for crude oil, natural gas, and refined products |
| API | https://www.eia.gov/opendata/ |
| License | Public domain (U.S. Government work) |
| License URL | https://www.eia.gov/about/copyrights_reuse.php |
| Redistribution | Allowed; acknowledgment requested |
| Attribution | `Source: U.S. Energy Information Administration (EIA).` |

EIA series included:

| Series ID | Instrument | Market | Unit |
|---|---|---|---|
| `RWTC` | WTI Crude Oil Spot | Cushing, OK | USD/barrel |
| `RBRTE` | Brent Crude Oil Spot | Europe | USD/barrel |
| `RNGWHHD` | Henry Hub Natural Gas Spot | Henry Hub, LA | USD/MMBtu |
| `EER_EPMRU_PF4_RGC_DPG` | Conventional Regular Gasoline Spot | U.S. Gulf Coast | USD/gallon |
| `EER_EPMRU_PF4_Y35NY_DPG` | Conventional Regular Gasoline Spot | New York Harbor | USD/gallon |
| `EER_EPMRR_PF4_Y05LA_DPG` | Reformulated RBOB Regular Gasoline Spot | Los Angeles | USD/gallon |
| `EER_EPD2DXL0_PF4_RGC_DPG` | Ultra-Low Sulfur No. 2 Diesel Spot | U.S. Gulf Coast | USD/gallon |
| `EER_EPD2DC_PF4_Y05LA_DPG` | CARB Diesel Spot | Los Angeles | USD/gallon |
| `EER_EPD2F_PF4_Y35NY_DPG` | No. 2 Heating Oil Spot | New York Harbor | USD/gallon |
| `EER_EPJK_PF4_RGC_DPG` | Kerosene-Type Jet Fuel Spot | U.S. Gulf Coast | USD/gallon |
| `EER_EPLLPA_PF4_Y44MB_DPG` | Propane Spot | Mont Belvieu, TX | USD/gallon |

---

## 2. Energy-Charts (Fraunhofer ISE) / Bundesnetzagentur | SMARD.de

| Field | Value |
|---|---|
| Data | European day-ahead electricity prices |
| API | https://api.energy-charts.info/ (`/price` endpoint) |
| Upstream | Bundesnetzagentur \| SMARD.de |
| License | CC BY 4.0 |
| License URL | https://creativecommons.org/licenses/by/4.0/ |
| Redistribution | Allowed with attribution and a change notice |
| Attribution | `Day-ahead price data: Bundesnetzagentur \| SMARD.de, retrieved via the Energy-Charts API (Fraunhofer ISE), licensed under CC BY 4.0.` |

Bidding zones included (all explicitly labeled CC BY 4.0 by the Energy-Charts
OpenAPI specification):

`DE-LU`, `FR`, `NL`, `BE`, `AT`, `CH`, `CZ`, `DK1`, `DK2`, `HU`, `IT-North`,
`NO2`, `PL`, `SE4`, `SI`

`DE-AT-LU` is deliberately omitted because it overlaps `DE-LU` and `AT`.

### Automated license check

Every `/price` response includes a `license_info` field. The pipeline calls
`ensure_redistributable()` on every response and aborts rather than publishing
if that field no longer says CC BY 4.0. This protects the dataset if upstream
terms change.

SMARD notes that it is reviewing legal licensing questions relating to
wholesale prices (HEDWIG proceedings). The Energy-Charts OpenAPI currently
labels the zones above as CC BY 4.0. Keep the automated check in place and
re-check the OpenAPI description periodically.

Links:

- https://www.smard.de/en/datennutzung
- https://creativecommons.org/licenses/by/4.0/

---

## Changes made by this pipeline

- Timestamps are converted to UTC.
- Data is reshaped into one tidy long table
  (`energy_trading_prices`), with per-source extracts
  (`eia_spot_prices`, `power_day_ahead_prices`).
- The sampling resolution actually covered by each row is recorded in the
  `Frequency` column (for example `hourly` or `15min`). A single zone can change
  resolution over time, so this is derived per row rather than per zone.
- Calendar helper columns (`Year`, `Month`, `Day`, `Hour`, `Minute`) are derived.
- Descriptive instrument and market names are added for EIA series.
- Price values are **not** modified.

---

## Sources deliberately excluded

| Source | Reason |
|---|---|
| FRED (Federal Reserve Bank of St. Louis) | Terms restrict archiving, redistribution, and commercial reuse. The EIA originals are used instead. https://fred.stlouisfed.org/legal/ |
| ENTSO-E day-ahead prices (Article 12.1.D) | Not on ENTSO-E's published CC BY 4.0 free-reuse list. |
| AEMO (Australia) | Copyrighted; redistribution requires permission. |
| EPEX SPOT / EEX / Nord Pool / ICE / CME | Commercial licenses; a paid data license is required. |

See `../ENERGY_DATA_LICENSE_AUDIT.md` for the full audit.

---

## How to attribute

When reusing this dataset, include:

```text
Energy Trading Prices (CC BY 4.0)

U.S. spot prices: Source: U.S. Energy Information Administration (EIA).
European day-ahead electricity prices: Bundesnetzagentur | SMARD.de, retrieved
via the Energy-Charts API (Fraunhofer ISE), licensed under CC BY 4.0
(https://creativecommons.org/licenses/by/4.0/).

Changes: timestamps converted to UTC; tables reshaped to a tidy long format;
original sampling resolution recorded. Price values are unchanged.
```
