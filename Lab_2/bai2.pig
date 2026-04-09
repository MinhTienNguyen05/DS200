data = LOAD 'output/bai1.csv' USING PigStorage(',') AS (
    id:chararray,
    word:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

grouped_words = GROUP data BY word;

word_counts = FOREACH grouped_words GENERATE group AS word, COUNT(data) AS freq;
words_above_500 = FILTER word_counts BY freq > 500;
words_ordered = ORDER words_above_500 BY freq DESC;

-- STORE words_ordered INTO 'output/bai2_word_freq' USING PigStorage(',');
samples = LIMIT words_ordered 10;
DUMP samples;

raw_data = LOAD 'data/hotel-review.csv' USING PigStorage(';') AS (
    id:chararray,
    review:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

valid_data = FILTER raw_data BY review IS NOT NULL AND review != '';

grouped_category = GROUP valid_data BY LOWER(category);
count_category = FOREACH grouped_category GENERATE
    group AS category,
    COUNT(valid_data) AS total_comments;

count_category_ordered = ORDER count_category BY total_comments DESC;

STORE count_category_ordered INTO 'output/bai2_category_count' USING PigStorage(',');
-- samples = LIMIT count_category_ordered 10;
-- DUMP samples;


grouped_aspect = GROUP valid_data BY LOWER(aspect);
count_aspect = FOREACH grouped_aspect GENERATE
    group AS aspect,
    COUNT(valid_data) AS total_comments;

count_aspect_ordered = ORDER count_aspect BY total_comments DESC;

STORE count_aspect_ordered INTO 'output/bai2_aspect_count' USING PigStorage(',');
-- samples = LIMIT count_aspect_ordered 10;
-- DUMP samples;