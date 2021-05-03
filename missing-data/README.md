# Missing Data Task

For technical reasons, the daily processing of one of our datasets has been split into 17 parts (0-f + s). The size of the dataset is expected to grow over time. We keep the latest iterations of the dataset around (assume we keep the last 7 days worth of data around), as well as the data for the first of the month for each month.

One day a developer notices that the total amount of data appears to have decreased over the course of 6 months. Furthermore, that developer has found a specific date (20210224) where all 17 spark applications seemingly finished without error, but some output files appear to be missing. On all other days that we still have data from, all files appear to be present.

- What can we do to regenerate the missing data?
- What short term steps can we take to ensure that we don't have an on-going data leak?
- How would you approach the problem of figuring out what is going on (i.e. coming up with a long term solution)?

the 17 jobs write to separate locations, for instance job 0 will write to 

```text
/daily-unified-mapping-aggregation-updated-prior/0/<date>/
```

while job 1 will write to 

```text
/daily-unified-mapping-aggregation-updated-prior/1/<date>/
```

In [success-markers](success-markers), you can find the success markers from each of the 17 jobs for the anomalous date 20210224.
