data = LOAD 'output/bai1.csv' USING PigStorage(',') AS (
    word:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);


group_cat_word = GROUP data BY (category, word);
count_cat_word = FOREACH group_cat_word GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(data) AS total_freq;

group_cat = GROUP count_cat_word BY category;

top5_per_cat = FOREACH group_cat {
    sorted_words = ORDER count_cat_word BY total_freq DESC;
    top_words = LIMIT sorted_words 5;
    clean_words = FOREACH top_words GENERATE word, total_freq;

    GENERATE group AS category, FLATTEN(clean_words);
};

-- STORE top5_per_cat INTO 'output/bai5' USING PigStorage(',');

samples = LIMIT top5_per_cat 10;
DUMP samples;

