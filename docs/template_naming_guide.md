# Template Naming Guide

## Goal

This document standardizes template naming so templates from different sources can be managed and parsed consistently.

## Naming Standard

Template id format:

TPL_{DOMAIN}_{SIGNAL}_{SHAPE}_V{MAJOR}

Validation regex:

^TPL_[A-Z0-9]+(?:_[A-Z0-9]+){2,}_V[0-9]+$

Segment rules:

1. TPL: fixed prefix.
2. DOMAIN: high-level signal domain, such as GROUP, TS, FUND.
3. SIGNAL: signal intent keyword.
4. SHAPE: expression structure keyword.
5. V{MAJOR}: integer major version.

## Mapping from Original Template Style to Standardized Template Id

1.
   group_op(ts_op(field, day) / ts_op(field, day), group)
   - TPL_GROUP_RATIO_TS2_V1
2.
   group_rank(ts_zscore(divide(actual-forecast, abs(forecast)), d), group)
   - TPL_GROUP_SURPRISE_ZSCORE_V1
3.
   group_rank(ts_delta(forecast, d), group)
   - TPL_GROUP_FORECAST_DELTA_V1
4.
   group_rank(ts_corr(actual, forecast, d), group)
   - TPL_GROUP_AF_CORR_V1
5.
   group_compare_op(sign + ts_compare_op(company_fundamentals, days), group)
   - TPL_GROUP_COMPARE_TS_V1
6.
   group_compare_op(ts_position_op(company_fundamentals, days), group)
   - TPL_GROUP_POSITION_TS_V1
7.
   group_compare_op(sign + ts_risk_op(company_fundamentals, days), group)
   - TPL_GROUP_RISK_TS_V1
8.
   time_series_op(profit_field / size_field, days)
   - TPL_TS_RATIO_FIELDPAIR_V1
9.
   group_op(put_greek - call_greek, grouping_data)
   - TPL_GROUP_GREEK_SPREAD_V1
10.
    group_op(ts_op(datafield, day), group)
    - TPL_GROUP_TS_BASIC_V1
11.
   group_rank(ts_smooth_op(iv_mean_field - hv_field, smooth_days), group)
   - TPL_GROUP_IVHV_SMOOTH_V1

## Machine-readable Catalog

Template definitions are stored in template_catalog.json. The generation module parses this file directly.

Catalog fields:

1. naming: standard and regex for id validation.
2. templates: list of template objects.
3. template.expression: rendered expression skeleton with placeholders.
4. template.slots: placeholder definitions, either fixed values or dataset_field source.
5. template.constraints.not_equal: slot combinations that must not be equal.

## Main Generator Integration

main.py now supports template-driven generation with these key parameters:

1. --template-doc: template catalog path.
2. --template-ids: comma-separated selected template ids, or ALL.
3. --slot-overrides-file: JSON file for slot override.
4. --operators-doc: operator dictionary for operator slot validation.

When generating factors:

1. Data fields are fetched from the selected dataset.
2. Selected templates are validated by naming regex.
3. Operator slots are validated against the operator dictionary.
4. Placeholders are expanded via Cartesian product with constraints.
5. Generated factors are written to batch files for backtesting.
