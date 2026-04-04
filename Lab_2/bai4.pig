data = LOAD 'output/bai1.csv' USING PigStorage(',') AS (
    word:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

data_pos = FILTER data by sentiment == 'positive';
group_pos_cat_word = GROUP data_pos BY (category, word);
count_pos_cat_word = FOREACH group_pos_cat_word GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(data_pos) AS total_pos;

group_pos_cat = GROUP count_pos_cat_word BY category;

top5_pos_per_cat = FOREACH group_pos_cat {
    sorted_words = ORDER count_pos_cat_word BY total_pos DESC;
    top_words = LIMIT sorted_words 5;
    clean_words = FOREACH top_words GENERATE word, total_pos;

    GENERATE group AS category, FLATTEN(clean_words);
};

-- STORE top5_pos_per_cat INTO 'output/bai4_top5_positive' USING PigStorage(',');

samples = LIMIT top5_pos_per_cat 10;
DUMP samples;

data_neg = FILTER data by sentiment == 'negative';
group_neg_cat_word = GROUP data_neg BY (category, word);
count_neg_cat_word = FOREACH group_neg_cat_word GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(data_neg) AS total_neg;

group_neg_cat = GROUP count_neg_cat_word BY category;

top5_neg_per_cat = FOREACH group_neg_cat {
    sorted_words = ORDER count_neg_cat_word BY total_neg DESC;
    top_words = LIMIT sorted_words 5;
    clean_words = FOREACH top_words GENERATE word, total_neg;

    GENERATE group AS category, FLATTEN(clean_words);
};

-- STORE top5_neg_per_cat INTO 'output/bai4_top5_negative' USING PigStorage(',');

samples = LIMIT top5_neg_per_cat 10;
DUMP samples;