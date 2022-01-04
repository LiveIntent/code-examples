# Introduction

This page contains a description of an exercise used for Graph BI positions in LiveIntent. The purpose of the exercise is to demonstrate an ability to understand a business problem, work with a dataset related to the business problem and make visualisations that would help a business owner make decisions.


# Problem

LiveIntent can acquire a license from a partner to use a set of identifiers to provide services for other partners. An identifier can only be used in those services if a license is obtained. Currently LiveIntent has an agreement with one such provider (LiveRamp) and is considering whether it should replace that partner with one of two other partners, Audience Accuity and TowerData, or maybe rely on two or three of them. To better inform this decision, a sampled dataset has been constructed with two tables and the following schemas:

```
-- contains a set of identifiers and for each identifier the number of opens, clicks and conversions has been calculated
-- can be joined using the "identifier column"

CREATE TABLE "identifier_info" (
	"identifier"	TEXT,
	"opens"	INTEGER,
	"clicks"	INTEGER,
	"conversions"	INTEGER
)

-- contains which partner(s) can provide licenses for a given identifier
-- each row means that the partner can provide a license for the given identifier
-- can be joined using the "identifier column"
-- the partners can be mapped from the license column in the following way
-- AudienceAcuityMain -> Audience Accuity
-- AudienceAcuityPair -> Audience Accuity
-- LiveRampPelFile -> LiveRamp
-- TowerData -> TowerData
-- a license for a given identifier can potentially be obtained from multiple partners

CREATE TABLE "license_info" (
	"identifier"	TEXT,
	"license"	TEXT
)
```

The dataset is available at [database](bi-exercise.db) which is a sqlite database and can be queried using [DB Browser for SQ lite](https://sqlitebrowser.org/dl/)

Please create one or more visualisations that would highlight how the choice of a new partner would affect available opens, clicks and conversions that could be used for provding services to other partners. A reasonable assumption is that the more opens, clicks and conversions that are available for the services the better the service will perform. Feel free to use all the tools you are comfortable with
