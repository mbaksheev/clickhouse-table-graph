# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
## 0.4.0
### Added
- TLS support for ClickHouse connection configuration. Thank you to [@FulgerX2007](https://github.com/FulgerX2007)
## 0.3.0
### Added
- Added extracting dependencies from [dictionaries functions](https://clickhouse.com/docs/sql-reference/functions/ext-dict-functions);

## 0.2.0
### Added
- Added extracting dependencies from JOIN clause for Materialized Views;

## 0.1.1
### Fixed
- Fixed detecting Distributed table dependencies for case when the sharding key and policy name specified in Distributed table parameters [issue#3](https://github.com/mbaksheev/clickhouse-table-graph/issues/3);

## 0.1.0
### Added
- Clickhouse table graph generator;
