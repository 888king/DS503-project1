AccessLog = LOAD 'AccessLog.csv' USING PigStorage(',') as (AccessId:int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime:int);

Access_report = FOREACH AccessLog GENERATE ByWho, WhatPage;
Grouped = GROUP Access_report by (ByWho,WhatPage);


visitor_page_count = FOREACH Grouped GENERATE FLATTEN(group) as (ByWho,WhatPage), COUNT(Access_report.WhatPage) as cnt;


visit_groups = GROUP visitor_page_count by ByWho;

out = FOREACH visit_groups GENERATE group, COUNT(visitor_page_count.WhatPage), SUM(visitor_page_count.cnt);
store out into 'job5';
