raw_data = LOAD '/home/bunble_05/hotel-review.csv' USING PigStorage(';') AS (
    id:chararray,
    comment:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

data_valid = FOREACH raw_data GENERATE
    aspect,
    LOWER(sentiment) AS sentiment;

data_valid = FILTER data_valid BY sentiment IS NOT NULL AND sentiment != '';

-- Xử lý NEGATIVE
neg_data   = FILTER data_valid BY sentiment == 'negative';
neg_group  = GROUP neg_data BY aspect;
neg_count  = FOREACH neg_group GENERATE 
                 group AS aspect, 
                 COUNT(neg_data) AS num_neg;

neg_all    = GROUP neg_count ALL;
neg_top1   = FOREACH neg_all {
    result = TOP(1, 1, neg_count); 
    GENERATE FLATTEN(result);
};

STORE neg_top1 INTO '/home/bunble_05/lab02/output/bai3_most_negative' USING PigStorage(';');

-- Xử lý POSITIVE
pos_data   = FILTER data_valid BY sentiment == 'positive';
pos_group  = GROUP pos_data BY aspect;
pos_count  = FOREACH pos_group GENERATE 
                 group AS aspect, 
                 COUNT(pos_data) AS num_pos;

pos_all    = GROUP pos_count ALL;
pos_top1   = FOREACH pos_all {
    result = TOP(1, 1, pos_count);
    GENERATE FLATTEN(result);
};

STORE pos_top1 INTO '/home/bunble_05/lab02/output/bai3_most_positive' USING PigStorage(';');