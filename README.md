# Spark-Datawarehousing
Spark based datawarehousing including cdc and scd2
Basically we are loading the input dataset as Spark dataframe.

Generating a MD5 for all the non key cols for comparing/detecting change with existing data

We are identifying new records based on left join with existing(table in actual scenario) data based on key col and taking out all records
which are new.(using Null condition for key col in right table)

Next we are doing change detection for same records existing in both source and target and filtering out record where there is some change via MD5 col

Finally we are taking unioin of 2 dtaset and merging with existing data.


