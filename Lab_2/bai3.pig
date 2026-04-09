data_raw = LOAD 'data/hotel-review.csv' USING PigStorage(';') AS (
    id:chararray,
    review:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

data = FILTER data_raw BY review IS NOT NULL AND review != '';
data_clean = FOREACH data GENERATE
    LOWER(aspect) AS aspect,
    LOWER(sentiment) AS sentiment;


data_pos = FILTER data_clean BY sentiment == 'positive';
group_pos = GROUP data_pos BY aspect;
count_pos = FOREACH group_pos GENERATE group AS aspect, COUNT(data_pos) AS total_pos;
order_pos = ORDER count_pos BY total_pos DESC;
top_pos = LIMIT order_pos 1;

-- DUMP top_pos;

STORE top_pos INTO 'output/bai3_top_pos' USING PigStorage(',');

data_neg = FILTER data_clean BY sentiment == 'negative';
group_neg = GROUP data_neg BY aspect;
count_neg = FOREACH group_neg GENERATE group AS aspect, COUNT(data_neg) AS total_neg;
order_neg = ORDER count_neg BY total_neg DESC;
top_neg = LIMIT order_neg 1;

-- DUMP top_neg;

STORE top_neg INTO 'output/bai3_top_neg' USING PigStorage(',');