data = LOAD 'output/bai1.csv' USING PigStorage(',') AS (
    word:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

data_pos = FILTER data by sentiment == 'positive';
group_pos = GROUP data_pos BY aspect;
count_pos = FOREACH group_pos GENERATE group AS aspect, COUNT(data_pos) AS total_pos;
order_pos = ORDER count_pos BY total_pos DESC;
top_pos = LIMIT order_pos 1;

-- STORE top_pos INTO 'output/bai3_top_pos' USING PigStorage(',');
samples = LIMIT top_pos 1;
DUMP samples;


data_neg = FILTER data by sentiment == 'negative';
group_neg = GROUP data_neg BY aspect;
count_neg = FOREACH group_neg GENERATE group AS aspect, COUNT(data_neg) AS total_neg;
order_neg = ORDER count_neg BY total_neg DESC;
top_neg = LIMIT order_neg 1;

-- STORE top_neg INTO 'output/bai3_top_neg' USING PigStorage(',');

samples = LIMIT top_neg 1;
DUMP samples;