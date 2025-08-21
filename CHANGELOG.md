# Changelog

## [0.6.0](https://github.com/erikmunkby/dbt-toolbox/compare/v0.5.1...v0.6.0) (2025-08-21)


### Features

* docs as mcp tool ([74c1a73](https://github.com/erikmunkby/dbt-toolbox/commit/74c1a732d263a3cfb94035577e6facfac65a3a52))


### Bug Fixes

* automatic search for closest yaml ([68fbb5b](https://github.com/erikmunkby/dbt-toolbox/commit/68fbb5b1522fbfd1a2c811cd1d74c216799ed168))
* compacted settings output ([b01aeed](https://github.com/erikmunkby/dbt-toolbox/commit/b01aeed7496ee0f231e20703e796326e3de7a460))
* pbcopy in ci ([797f270](https://github.com/erikmunkby/dbt-toolbox/commit/797f27006e29882b9ca148c2803e922f6d5f267a))

## [0.5.1](https://github.com/erikmunkby/dbt-toolbox/compare/v0.5.0...v0.5.1) (2025-08-18)


### Bug Fixes

* config kwarg update ([2742904](https://github.com/erikmunkby/dbt-toolbox/commit/27429047eb9708b27ca44b8235a4f36d8a12391a))

## [0.5.0](https://github.com/erikmunkby/dbt-toolbox/compare/v0.4.0...v0.5.0) (2025-08-17)


### Features

* add dt build as CLI tool ([4822f33](https://github.com/erikmunkby/dbt-toolbox/commit/4822f339b1d8ea4232078cd727e96a14a8ad5fb8))
* mcp server with analyze tool ([a0670e0](https://github.com/erikmunkby/dbt-toolbox/commit/a0670e04a8955be936e346c42d93c05e28f6ffbd))


### Bug Fixes

* instant code change reflection ([936f531](https://github.com/erikmunkby/dbt-toolbox/commit/936f531fcaba00fc22e328c88d2f9743aeee2be1))
* support model configs ([e36ca58](https://github.com/erikmunkby/dbt-toolbox/commit/e36ca58cab13ed30463fb6f93ed0e3dc0d745a04))

## [0.4.0](https://github.com/erikmunkby/dbt-toolbox/compare/v0.3.2...v0.4.0) (2025-08-15)


### Features

* track execution times ([7aeccf3](https://github.com/erikmunkby/dbt-toolbox/commit/7aeccf3b4b15741308ca5ed77d2bf87aa0a2fb4a))


### Bug Fixes

* collect model execution time ([40e6729](https://github.com/erikmunkby/dbt-toolbox/commit/40e67290b2395e34e2c214dbbe5aacfad8e6c212))

## [0.3.2](https://github.com/erikmunkby/dbt-toolbox/compare/v0.3.1...v0.3.2) (2025-08-13)


### Bug Fixes

* macro changed persistence ([047ed28](https://github.com/erikmunkby/dbt-toolbox/commit/047ed282f6d1d0836bddf9e3d9f4807e4725b918))

## [0.3.1](https://github.com/erikmunkby/dbt-toolbox/compare/v0.3.0...v0.3.1) (2025-08-12)


### Bug Fixes

* update docs ([55cc56b](https://github.com/erikmunkby/dbt-toolbox/commit/55cc56b60362ead919b6e3cfb33e588f1c37718c))

## [0.3.0](https://github.com/erikmunkby/dbt-toolbox/compare/v0.2.1...v0.3.0) (2025-08-10)


### Features

* **settings:** ignore model validation ([2378267](https://github.com/erikmunkby/dbt-toolbox/commit/23782676e7049a5a335f6c31dbbc5b6a66eb70af))

## [0.2.1](https://github.com/erikmunkby/dbt-toolbox/compare/v0.2.0...v0.2.1) (2025-08-10)


### Bug Fixes

* analyze cte columns ([9bef9ca](https://github.com/erikmunkby/dbt-toolbox/commit/9bef9ca89d9031769e9f5a3f044f4d6f619c160b))
* complex column lineage ([b68e89d](https://github.com/erikmunkby/dbt-toolbox/commit/b68e89de153ffbeb14332cdb7f7ad87df27896c9))
* handle CTE column references ([e1ffd0b](https://github.com/erikmunkby/dbt-toolbox/commit/e1ffd0b55a0160e767fbbc80dea1888fccca790b))
* ignore CTEs in lineage validation ([6cb999e](https://github.com/erikmunkby/dbt-toolbox/commit/6cb999e39b39b7b5eb5f03691cfd990b0c2b1eac))
* macro changed not detected ([18c76d8](https://github.com/erikmunkby/dbt-toolbox/commit/18c76d8076dc49711315329b7fbcc7f4bfcd7148))
* major settings refactoring ([38e2b02](https://github.com/erikmunkby/dbt-toolbox/commit/38e2b023108a8fe204ea8baece46dca375e7ab47))
* mixed select * cte ([aa9eef8](https://github.com/erikmunkby/dbt-toolbox/commit/aa9eef87d75b0334783043500856d5c4166ef32f))
* model analysis and caching ([99313af](https://github.com/erikmunkby/dbt-toolbox/commit/99313af40b46e4dea30fd769b836eeae9eca519a))
* recursive column parsing ([7df78eb](https://github.com/erikmunkby/dbt-toolbox/commit/7df78eb8f76d0080d4372afc101a9d737d272bba))
* refactor model caching ([ba3eabb](https://github.com/erikmunkby/dbt-toolbox/commit/ba3eabb53a186419b0ecb73f995dbb18bd30cab9))

## [0.2.0](https://github.com/erikmunkby/dbt-toolbox/compare/v0.1.2...v0.2.0) (2025-07-28)


### Features

* **cli:** add lineage validation to build/run ([a0ecfb1](https://github.com/erikmunkby/dbt-toolbox/commit/a0ecfb15a1de4e07750917cdac52b57f395d8122))
* extend analysis functionality with column lineage validation ([cda2251](https://github.com/erikmunkby/dbt-toolbox/commit/cda2251f750b29699d5ffb34505e820a25eef504))


### Bug Fixes

* add support for column lineage validation ([312dc0b](https://github.com/erikmunkby/dbt-toolbox/commit/312dc0b9477caf87334dfa6bf7e0964370264e3d))
* build lineage support for seeds ([2ab8af9](https://github.com/erikmunkby/dbt-toolbox/commit/2ab8af9d5eb5b2c109f522dd4fede48a745dc0cb))
* easier model access ([477e7af](https://github.com/erikmunkby/dbt-toolbox/commit/477e7af8c5874eec385faf7f91859d4615d2b0df))

## [0.1.2](https://github.com/erikmunkby/dbt-toolbox/compare/v0.1.1...v0.1.2) (2025-07-25)


### Bug Fixes

* pyproject description ([6a85b92](https://github.com/erikmunkby/dbt-toolbox/commit/6a85b92d43ac2d316e5aaa401b01e365250e9529))

## [0.1.1](https://github.com/erikmunkby/dbt-toolbox/compare/v0.1.0...v0.1.1) (2025-07-25)


### Bug Fixes

* link description to pypi ([bb4f629](https://github.com/erikmunkby/dbt-toolbox/commit/bb4f6294f04ab30c3474beba0ea0756f95f5c634))

## 0.1.0 (2025-07-25)


### Bug Fixes

* make methods public ([f6a30ba](https://github.com/erikmunkby/dbt-toolbox/commit/f6a30ba99b4502f7702275af7dd4251bb77b9b8f))
* **settings:** change toml lib to support python 3.10 ([6c38116](https://github.com/erikmunkby/dbt-toolbox/commit/6c38116656e042e9ac81fc46b235a544a8e78841))


### Documentation

* fix incorrect readme ([a05bc7b](https://github.com/erikmunkby/dbt-toolbox/commit/a05bc7be5090b3f92165f6f21b7b89fd1989afbb))
