raw_data = LOAD '/home/bunble_05/hotel-review.csv' USING PigStorage(';') AS (
    id:int,
    comment:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

data_valid = FOREACH raw_data GENERATE
    aspect,
    LOWER(sentiment) AS sentiment;

data_valid = FILTER data_valid BY sentiment IS NOT NULL AND sentiment != '';

-- Aspect có nhiều đánh giá NEGATIVE nhất
neg_data   = FILTER data_valid BY sentiment == 'negative';
neg_group  = GROUP neg_data BY aspect;
neg_count  = FOREACH neg_group GENERATE
                 group          AS aspect,
                 COUNT(neg_data) AS num_neg;
neg_sorted = ORDER neg_count BY num_neg DESC;
neg_top1   = LIMIT neg_sorted 1;

STORE neg_sorted INTO '/home/bunble_05/lab02/output/bai3_negative_aspects' USING PigStorage(';');
STORE neg_top1   INTO '/home/bunble_05/lab02/output/bai3_most_negative' USING PigStorage(';');

-- Aspect có nhiều đánh giá POSITIVE nhất
pos_data   = FILTER data_valid BY sentiment == 'positive';
pos_group  = GROUP pos_data BY aspect;
pos_count  = FOREACH pos_group GENERATE
                 group          AS aspect,
                 COUNT(pos_data) AS num_pos;
pos_sorted = ORDER pos_count BY num_pos DESC;
pos_top1   = LIMIT pos_sorted 1;

STORE pos_sorted INTO '/home/bunble_05/lab02/output/bai3_positive_aspects' USING PigStorage(';');
STORE pos_top1   INTO '/home/bunble_05/lab02/output/bai3_most_positive' USING PigStorage(';');