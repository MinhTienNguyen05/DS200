data_raw = LOAD 'data/hotel-review.csv' USING PigStorage(';') AS (
    id:chararray, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);


data_valid = FILTER data_raw BY review IS NOT NULL AND review != '';


data_clean = FOREACH data_valid GENERATE
    TRIM(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(LOWER(review), '[-^]', ' '),
                    '\\p{Punct}', ''
                ),
                '[0-9]', ''
            ),
            '\\s+', ' '
        )
    ) AS review,
    LOWER(category) AS category,
    LOWER(aspect) AS aspect,
    LOWER(sentiment) AS sentiment;


data_tokenized = FOREACH data_clean GENERATE
    TOKENIZE(review) AS words, category, aspect, sentiment;


stopwords = LOAD 'data/stopwords.txt' AS (stopword:chararray);

words_flattened = FOREACH data_tokenized GENERATE
    FLATTEN(words) AS word, category, aspect, sentiment;

joined_data = JOIN words_flattened BY word LEFT OUTER, stopwords BY stopword;

data_filtered = FILTER joined_data BY stopwords::stopword IS NULL;


result = FOREACH data_filtered GENERATE
    words_flattened::word AS word,
    words_flattened::category AS category,
    words_flattened::aspect AS aspect,
    words_flattened::sentiment AS sentiment;


STORE result INTO 'output/bai1' USING PigStorage(',');

-- samples = LIMIT result 10;
-- DUMP samples;