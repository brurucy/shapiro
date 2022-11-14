# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- `ChibiDatalog` is now paralleized

### Changed
- Insertion and storage relies on `boxed` types

## [0.1.0] - 2022-09-06
### Added
- Skeleton of the project
- `ChibiDatalog` as the prototypical, simplest-as-possible SLD-based positive datalog

[Unreleased]: https://github.com/brurucy/shapiro/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/brurucy/shapiro/releases/tag/v0.4.0
[0.3.1]: https://github.com/brurucy/shapiro/releases/tag/v0.3.1	
[0.3.0]: https://github.com/brurucy/shapiro/releases/tag/v0.3.0
[0.2.0]: https://github.com/brurucy/shapiro/releases/tag/v0.2.0
[0.1.0]: https://github.com/brurucy/shapiro/releases/tag/v0.1.0
