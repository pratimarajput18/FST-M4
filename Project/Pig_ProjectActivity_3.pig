-- Load input file from HDFS
episodeTable = LOAD 'hdfs:///user/root/episodeVI_dialouges.txt' USING PigStorage('\t') AS (Name:chararray,Dialouge:chararray);
characters = GROUP episodeTable BY Name;
countByCharacter = FOREACH characters GENERATE $0 as character_name, COUNT($1) as no_of_dlg;
orderData = ORDER countByCharacter BY no_of_dlg DESC;
STORE orderData INTO 'hdfs:///user/root/Pig_Activity_1';
STORE orderData INTO '/root/Pig_Activity_1';