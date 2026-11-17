-- Experiment E1, Pig version of the report: orders and quoted amount per branch, day and status code.
-- Run:  pig -x mapreduce -param INPUT=/lake/tmp/e1/x1 -param OUTPUT=/lake/tmp/e1_out_pig e1_orders.pig
-- '-tagPath' puts the path of the input file in front of the 95 exported columns; the branch is read from it.
o = LOAD '$INPUT/*/*' USING PigStorage(',', '-tagPath');
p = FOREACH o GENERATE REGEX_EXTRACT((chararray)$0, 'branch=([A-Z])', 1) AS branch,
                       SUBSTRING((chararray)$11, 0, 10) AS day,
                       (chararray)$18 AS status,
                       (double)$69 AS quoted;
g = GROUP p BY (branch, day, status);
r = FOREACH g GENERATE FLATTEN(group), COUNT(p) AS orders, SUM(p.quoted) AS quoted;
STORE r INTO '$OUTPUT';
