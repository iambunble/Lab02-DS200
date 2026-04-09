data = LOAD '/user/hadoop/lab02/hotel-review.csv' USING PigStorage(';') AS (
    id:int,
    comment:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

data = FOREACH data GENERATE
    LOWER(comment) AS comment,
    category,
    aspect,
    sentiment;

data = FOREACH data GENERATE
    REPLACE(comment, '[,.&@!%+/\':><?]', '') AS comment,
    category,
    aspect,
    sentiment;

data = FOREACH data GENERATE
    REPLACE(comment, '\\d+', '') AS comment,
    category,
    aspect,
    sentiment;

data = FOREACH data GENERATE
    REPLACE(comment, '[0-9]', '') AS comment,
    category,
    aspect,
    sentiment;

data = FOREACH data GENERATE
    REPLACE(comment, '[-^]', ' ') AS comment,
    category,
    aspect,
    sentiment;

data = FILTER data BY comment IS NOT NULL;
data = FILTER data BY comment != '';

words = FOREACH data GENERATE
    FLATTEN(TOKENIZE(TRIM(comment))) AS word,
    category,
    aspect,
    sentiment;

stopwords = LOAD '/user/hadoop/lab02/stopwords.txt' USING PigStorage() AS (
    word:chararray
);

joined_words = JOIN words BY word LEFT OUTER, stopwords BY word;

filtered_words = FILTER joined_words BY stopwords::word IS NULL;

result = FOREACH filtered_words GENERATE
    words::word AS word,
    words::category AS category,
    words::aspect AS aspect,
    words::sentiment AS sentiment;

samples = LIMIT result 10;
DUMP samples;

STORE result INTO '/user/hadoop/lab02/output/bai1' USING PigStorage(',');