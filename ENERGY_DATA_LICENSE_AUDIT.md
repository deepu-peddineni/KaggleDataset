# Energy Data License Audit

**Audit date:** 2026-11-08
**Scope:** open data sources that could be used for an energy-trading dataset and
published to Kaggle.
**Status:** informational only, not legal advice. Re-check the linked terms
before relying on them commercially.

## Summary

| Source | Data | License | Redistribution | Decision |
|---|---|---|---|---|
| U.S. Energy Information Administration (EIA) | Crude, gas, and refined-product spot prices | Public domain (U.S. Government work) | Allowed; acknowledgment requested | ✅ **Use** |
| Bundesnetzagentur \| SMARD.de via Energy-Charts API (Fraunhofer ISE) | European day-ahead electricity prices for 16 listed bidding zones | CC BY 4.0 | Allowed with attribution and change notice | ✅ **Use** |
| Ember | Electricity generation, emissions, and some price aggregates | CC BY 4.0 | Allowed with attribution | ✅ Optional |
| ENTSO-E Transparency Platform | Grid and market data | CC BY 4.0 **only** for the published free-reuse list | Only listed items | ⚠️ **Exclude day-ahead prices** |
| FRED (Federal Reserve Bank of St. Louis) | Economic time series | Restrictive terms; not an open-data license | Prohibited in many cases | ❌ **Do not publish** |
| AEMO (Australia) | NEM wholesale market data | Copyright; permission required | Restricted | ❌ **Exclude** |
| EPEX SPOT / EEX / Nord Pool / ICE / CME | Exchange prices and futures | Commercial licences | Requires a paid licence | ❌ **Exclude** |

## Details

### 1. U.S. Energy Information Administration (EIA) — ✅ approved

- Data: daily spot prices for WTI, Brent, Henry Hub natural gas, gasoline,
  diesel, heating oil, jet fuel, and propane via the EIA Open Data API v2.
- License: U.S. Government publications are in the public domain. EIA states
  that you may use and distribute its data, files, and databases, and asks for
  an acknowledgment such as "Source: U.S. Energy Information Administration".
- API terms: the API Terms of Service allow use of the API to retrieve and
  display EIA data and require attribution. They do not prohibit redistribution.
- Links:
  - https://www.eia.gov/about/copyrights_reuse.php
  - https://www.eia.gov/opendata/terms-of-service.php
- Required attribution: `Source: U.S. Energy Information Administration (EIA).`

### 2. SMARD.de / Energy-Charts — ✅ approved for listed zones

- Data: European day-ahead electricity prices.
- SMARD is operated by the German Federal Network Agency
  (Bundesnetzagentur). Its data-use page states that data from the market-data
  visualisations is licensed under CC BY 4.0 and should be attributed to
  "Bundesnetzagentur | SMARD.de".
- The Energy-Charts OpenAPI specification lists the bidding zones whose price
  data it labels CC BY 4.0 from Bundesnetzagentur | SMARD.de. It also states
  that data for other bidding zones is for private/internal use only.
- This pipeline restricts itself to the explicitly CC BY 4.0 zones and verifies
  the `license_info` field returned by every API response. If that field stops
  saying CC BY 4.0, the pipeline aborts instead of publishing.
- Bidding zones used: `DE-LU`, `FR`, `NL`, `BE`, `AT`, `CH`, `CZ`, `DK1`,
  `DK2`, `HU`, `IT-North`, `NO2`, `PL`, `SE4`, `SI`.
  (`DE-AT-LU` is omitted because it overlaps `DE-LU` and `AT`.)
- Links:
  - https://www.smard.de/en/datennutzung
  - https://api.energy-charts.info/ (OpenAPI: `/price` endpoint)
  - https://creativecommons.org/licenses/by/4.0/
- Required attribution:
  `Day-ahead price data: Bundesnetzagentur | SMARD.de, retrieved via the
  Energy-Charts API (Fraunhofer ISE), licensed under CC BY 4.0.`
- Change notice: timestamps are converted to UTC, the tables are reshaped to a
  tidy long format, and original resolution is recorded. Price values are not
  modified.
- Caution: SMARD notes that it is reviewing legal licensing questions relating
  to wholesale prices (HEDWIG proceedings). The Energy-Charts OpenAPI currently
  labels the zones above as CC BY 4.0. Keep the automated license check in place
  and re-check the OpenAPI description periodically.

### 3. Ember — ✅ optional

- Data: electricity generation, emissions, and some aggregate price series.
- License: CC BY 4.0 for all Ember content.
- Link: https://ember-energy.org/data/
- Attribution: `Source: Ember (CC BY 4.0).`

### 4. ENTSO-E Transparency Platform — ⚠️ day-ahead prices excluded

- ENTSO-E publishes a specific list of data available for free re-use under
  CC BY 4.0. The list is not the whole platform.
- The published list covers items such as load forecasts, cross-zonal capacity,
  physical flows, balancing reserves, and imbalance prices. It does **not**
  include day-ahead market prices (Article 12.1.D of Regulation 543/2013).
- Because day-ahead prices are outside the free-reuse list, this project does
  **not** ingest them from ENTSO-E. It uses SMARD/Energy-Charts for European
  power prices instead.
- Links:
  - https://transparencyplatform.zendesk.com/hc/en-us/articles/40921911218961
  - https://transparencyplatform.zendesk.com/hc/article_attachments/40921869379729
    (List of Data available for reuse, 18 Oct 2023)

### 5. FRED — ❌ do not publish

- FRED's terms describe access as being for personal, non-commercial use and
  prohibit, among other things, storing/caching/archiving FRED content,
  incorporating it in a database or compilation, and redistributing third-party
  proprietary content for commercial use without permission.
- A public Kaggle dataset is a redistribution/archive, so publishing FRED-sourced
  series is a licensing risk even when the underlying original source is a U.S.
  government agency.
- Links:
  - https://fred.stlouisfed.org/legal/
  - https://fred.stlouisfed.org/docs/api/terms_of_use.html
- **Existing repo risk:** `CrudeOil/crude_oil_brent.py` uses FRED
  `DCOILBRENTEU`, and `HenryHub/henry_hub_downloader.py` uses FRED `DHHNGSP`.
  Both underlying series are EIA data, so the clean fix is to fetch the EIA
  series directly:
  - Brent: EIA `RBRTE` on `petroleum/pri/spt`
  - Henry Hub: EIA `RNGWHHD` on `natural-gas/pri/fut`
  The new `EnergyTrading` pipeline already does this.

### 6. AEMO — ❌ excluded

- AEMO states that all AEMO material is protected by copyright under Australian
  law; permission is required for uses beyond the limited permissions it grants.
- Link: https://www.aemo.com.au/privacy-and-legal-notices/copyright-permissions

### 7. Exchange and futures data — ❌ excluded

- EPEX SPOT, EEX, Nord Pool, ICE, and CME data are commercial products. Public
  web pages are not a redistribution licence. A paid data licence is required
  before publishing them on Kaggle.

## Existing datasets in this repository

| Dataset | Current source | Assessment | Recommended action |
|---|---|---|---|
| `CrudeOil` | FRED `DCOILBRENTEU` | ❌ FRED terms restrict redistribution | Switch to EIA `RBRTE` and attribute EIA |
| `HenryHub` | FRED `DHHNGSP` | ❌ FRED terms restrict redistribution | Switch to EIA `RNGWHHD` and attribute EIA |
| `Gasoline` / `wti_crude_oil` | EIA API | ✅ Public domain; attribution required | Add EIA acknowledgment to description |
| `EuropeanElectricity` wholesale prices | Local `all_countries.csv`; original source not documented | ⚠️ Config claims CC0-1.0; provenance and licence are unverified | Document the original source and correct the licence |
| `EuropeanElectricity` interconnection | Ember | ⚠️ Ember is CC BY 4.0, but config claims CC0-1.0 | Set `CC-BY-4.0` and add Ember attribution |
| `Archive/` | Mixed historical files | Not part of the published pipeline | Do not publish without review |

## Attribution block for the new dataset

Include the following in the Kaggle dataset description and any redistributed
copy:

```text
Attribution
-----------
U.S. spot prices: Source: U.S. Energy Information Administration (EIA).
European day-ahead electricity prices: Bundesnetzagentur | SMARD.de, retrieved
via the Energy-Charts API (Fraunhofer ISE), licensed under CC BY 4.0
(https://creativecommons.org/licenses/by/4.0/).

Changes: timestamps converted to UTC; data reshaped into a tidy long table;
original sampling resolution recorded. Price values are unchanged.
```

The most restrictive upstream licence is CC BY 4.0, so the published dataset
should be listed as **CC-BY-4.0** on Kaggle even though the EIA portion is
public domain.
