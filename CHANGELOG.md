# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed

## [0.9.0] - 2022-04-06
### Changed
- `DifferentialIndexed` - fixed a correctness issue, where "filtering" predicates were ignored. No performance regressions.

## [0.8.0] - 2022-03-31
### Added
- `DifferentialIndexed` - a variant of `Differential` that uses a novel indexing scheme that **seems** to ensure optimal unification 
- `ChibiIndexed` - the same as above, but for `Chibi`.

## [0.7.0] - 2022-03-28
### Added
- `DifferentialDatalog` a differential-dataflow-based datalog engine.
- Non-indexed variations of `Database`, `Relation` and `Instance`.
- All reasoners confidently support non-linear positive datalog programs.

### Changed
- `Chibi` now does not use an Indexed relation, since it does not need it.
- Rethinking of interfaces altogether
- Refactor of all reasoners

## [0.6.0] - 2022-12-28
### Added
- A simple benchmarking harness

## [0.5.0] - 2022-11-23
### Added
- An implementation of the Delete-Rederive algorithm
- Materialization of programs

### Changed
- `Chibi` and `Simple` now have a `delete` method.
- `Chibi` and `Simple` are able to incrementally maintain evaluations.
- Refactored all APIs

## [0.4.0] - 2022-11-14
### Added
- String interning - 3x speed gains across the board

## [0.3.1] - 2022-10-16
### Changed
- `Cargo.toml` version corrected

## [0.3.0] - 2022-10-15
### Changed
- `SimpleDatalog` is generic over `Index`

## [0.2.0] - 2022-09-07
### Added
- `SimpleDatalog` very-fast parallel datalog engine that works by interpreting relational algebra
- `ChibiDatalog` is now parallelized

### Changed
- Insertion and storage relies on `boxed` types

## [0.1.0] - 2022-09-06
### Added
- Skeleton of the project
- `ChibiDatalog` as the prototypical, simplest-as-possible substitution-based positive datalog interpreter

[Unreleased]: https://github.com/brurucy/shapiro/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/brurucy/shapiro/releases/tag/v0.9.0
[0.8.0]: https://github.com/brurucy/shapiro/releases/tag/v0.8.0
[0.7.0]: https://github.com/brurucy/shapiro/releases/tag/v0.7.0
[0.6.0]: https://github.com/brurucy/shapiro/releases/tag/v0.6.0
[0.5.0]: https://github.com/brurucy/shapiro/releases/tag/v0.5.0
[0.4.0]: https://github.com/brurucy/shapiro/releases/tag/v0.4.0
[0.3.1]: https://github.com/brurucy/shapiro/releases/tag/v0.3.1	
[0.3.0]: https://github.com/brurucy/shapiro/releases/tag/v0.3.0
[0.2.0]: https://github.com/brurucy/shapiro/releases/tag/v0.2.0
[0.1.0]: https://github.com/brurucy/shapiro/releases/tag/v0.1.0
