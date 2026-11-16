#!/bin/bash
# 06 - HBase shell (create/put/get/scan) and ImportTsv (a MapReduce job) of a tiny CSV.
TEST=06_hbase; . "$(dirname "$0")/common.sh"
D=/user/bigdata/smoke_hbase

# start clean if an earlier run was interrupted
for t in smoke_hb smoke_hb_import; do
  printf "if exists '%s'\n  disable '%s'\n  drop '%s'\nend\n" $t $t $t | hbase shell -n >/dev/null 2>&1
done

run "hbase shell -n <<'HB'
create 'smoke_hb', 'cf'
put 'smoke_hb', 'r1', 'cf:name', 'alice'
put 'smoke_hb', 'r1', 'cf:city', 'Dubai'
put 'smoke_hb', 'r2', 'cf:name', 'bob'
get 'smoke_hb', 'r1'
scan 'smoke_hb'
HB"
must   "hbase shell exit code 0"
expect "get returns r1 cf:name" "cf:name +timestamp=[0-9]+, value=alice"
expect "scan returns r2" "r2 +column=cf:name, timestamp=[0-9]+, value=bob"
expect "scan found 2 rows" "^2 row\(s\)"

echo; echo "--- ImportTsv"
run "hdfs dfs -rm -r -f -skipTrash $D; hdfs dfs -mkdir -p $D"
run "printf 'k1,carol,Sydney\nk2,dave,Wollongong\nk3,erin,Dubai\n' > /tmp/smoke_people.csv && hdfs dfs -put -f /tmp/smoke_people.csv $D/people.csv"
run "echo \"create 'smoke_hb_import', 'cf'\" | hbase shell -n"
run "hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=, -Dimporttsv.columns=HBASE_ROW_KEY,cf:name,cf:city smoke_hb_import $D/people.csv"
must   "ImportTsv exit code 0"
expect "ImportTsv ran as a YARN application" "Submitted application application_[0-9]+_[0-9]+"
expect "ImportTsv job completed successfully" "completed successfully"
run "echo \"scan 'smoke_hb_import'\" | hbase shell -n"
expect "imported row k2" "k2 +column=cf:city, timestamp=[0-9]+, value=Wollongong"
expect "3 rows imported" "^3 row\(s\)"

echo; echo "--- clean up"
run "hbase shell -n <<'HB'
disable 'smoke_hb'
drop 'smoke_hb'
disable 'smoke_hb_import'
drop 'smoke_hb_import'
list
HB"
must "tables dropped"
run "hdfs dfs -rm -r -skipTrash $D"
rm -f /tmp/smoke_people.csv
finish
