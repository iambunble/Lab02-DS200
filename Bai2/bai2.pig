-- Load kết quả bài 1
cleaned = LOAD '/user/hadoop/lab02/output/bai1' USING PigStorage(',') AS (
    word:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
    );

-- 2.1 Tần số từ > 500 lần
word_group = GROUP cleaned BY word;
word_freq  = FOREACH word_group GENERATE
                 group          AS word,
                 COUNT(cleaned) AS freq;
top_words        = FILTER word_freq BY freq > 500;
top_words_sorted = ORDER top_words BY freq DESC;

STORE top_words_sorted INTO '/user/hadoop/lab02/output/bai2_word_freq' USING PigStorage(';');

-- 2.2 Số bình luận theo Category
raw_data = LOAD '/user/hadoop/lab02/hotel-review.csv' USING PigStorage(';') AS (
    id:int,
    comment:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
    );

id_category      = FOREACH raw_data GENERATE id, category;
id_category_dist = DISTINCT id_category;
cat_group        = GROUP id_category_dist BY category;
cat_count        = FOREACH cat_group GENERATE
                       group                   AS category,
                       COUNT(id_category_dist) AS num_reviews;
cat_sorted = ORDER cat_count BY num_reviews DESC;

STORE cat_sorted INTO '/user/hadoop/lab02/output/bai2_category' USING PigStorage(';');

-- 2.3 Số bình luận theo Aspect
id_aspect      = FOREACH raw_data GENERATE id, aspect;
id_aspect_dist = DISTINCT id_aspect;
asp_group      = GROUP id_aspect_dist BY aspect;
asp_count      = FOREACH asp_group GENERATE
                     group                  AS aspect,
                     COUNT(id_aspect_dist)  AS num_reviews;
asp_sorted = ORDER asp_count BY num_reviews DESC;

STORE asp_sorted INTO '/user/hadoop/lab02/output/bai2_aspect' USING PigStorage(';');