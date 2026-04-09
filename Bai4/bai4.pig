cleaned = LOAD '/user/hadoop/lab02/output/bai1' USING PigStorage(',') AS (
    word:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
    );

norm = FOREACH cleaned GENERATE
           word,
           category,
           LOWER(sentiment) AS sentiment;

-- Top 5 từ POSITIVE theo từng Category
pos_words        = FILTER norm BY sentiment == 'positive';
pos_cat_word_grp = GROUP pos_words BY (category, word);
pos_cat_word_cnt = FOREACH pos_cat_word_grp GENERATE
                       FLATTEN(group)   AS (category, word),
                       COUNT(pos_words) AS freq;
pos_by_cat = GROUP pos_cat_word_cnt BY category;
pos_top5   = FOREACH pos_by_cat {
    sorted = ORDER pos_cat_word_cnt BY freq DESC;
    top5   = LIMIT sorted 5;
    GENERATE group AS category, FLATTEN(top5);
};

STORE pos_top5 INTO '/user/hadoop/lab02/output/bai4_top5_positive' USING PigStorage(';');

-- Top 5 từ NEGATIVE theo từng Category
neg_words        = FILTER norm BY sentiment == 'negative';
neg_cat_word_grp = GROUP neg_words BY (category, word);
neg_cat_word_cnt = FOREACH neg_cat_word_grp GENERATE
                       FLATTEN(group)   AS (category, word),
                       COUNT(neg_words) AS freq;
neg_by_cat = GROUP neg_cat_word_cnt BY category;
neg_top5   = FOREACH neg_by_cat {
    sorted = ORDER neg_cat_word_cnt BY freq DESC;
    top5   = LIMIT sorted 5;
    GENERATE group AS category, FLATTEN(top5);
};

STORE neg_top5 INTO '/user/hadoop/lab02/output/bai4_top5_negative' USING PigStorage(';');