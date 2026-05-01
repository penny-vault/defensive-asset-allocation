# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.3] - 2026-05-01

### Changed
- Upgrade pvbt dependency to v0.8.1

## [0.1.2] - 2026-04-25

### Changed
- Upgrade pvbt dependency to v0.8.0
- Regenerate testdata snapshot for pvbt's v5 snapshot schema

### Fixed
- Test imports now reference `asset.BuyTransaction`/`SellTransaction` from pvbt's `asset` package, where they actually live

## [0.1.1] - 2026-04-23

### Changed
- Upgrade pvbt dependency to v0.7.7

## [0.1.0] - 2026-04-21

### Added
- Initial release of Defensive Asset Allocation strategy
- Momentum12631 scoring with canary asset crash protection
- Ginkgo test suite with AllocateSmartly preset for validation

[0.1.0]: https://github.com/penny-vault/defensive-asset-allocation/releases/tag/v0.1.0
[0.1.1]: https://github.com/penny-vault/defensive-asset-allocation/compare/v0.1.0...v0.1.1
[0.1.2]: https://github.com/penny-vault/defensive-asset-allocation/compare/v0.1.1...v0.1.2
[0.1.3]: https://github.com/penny-vault/defensive-asset-allocation/compare/v0.1.2...v0.1.3
