cleaned = LOAD '/user/hadoop/lab02/output/bai1' USING PigStorage(',') AS (
    word:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
    );

cat_word = FOREACH cleaned GENERATE category, word;

cat_word_grp = GROUP cat_word BY (category, word);
cat_word_cnt = FOREACH cat_word_grp GENERATE
                   FLATTEN(group)  AS (category, word),
                   COUNT(cat_word) AS freq;

by_cat = GROUP cat_word_cnt BY category;
top5_relevant = FOREACH by_cat {
    sorted = ORDER cat_word_cnt BY freq DESC;
    top5   = LIMIT sorted 5;
    GENERATE group AS category, FLATTEN(top5);
};

STORE top5_relevant INTO '/user/hadoop/lab02/output/bai5_top5_relevant' USING PigStorage(';');