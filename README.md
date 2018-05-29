# Inquiries_Active_Month
# of inquiries (by type) per # of active members by month

-- raw sql results do not include filled-in values for 'jira_issues.created_week'


-- use existing jira_issues in presto_looker_s3_operations.pdts2.LR_5WYQQJXM9D4ZJ72D68O2B_jira_issues
-- use existing jira_customvalues in presto_looker_s3_operations.pdts2.LR_5WY492XSL91CZWNVCRVJB_jira_customvalues
SELECT * FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering FROM (
SELECT *, MIN(z___rank) OVER (PARTITION BY "jira_issues.created_week") as z___min_rank FROM (
SELECT *, RANK() OVER (ORDER BY CASE WHEN z__pivot_col_rank=1 THEN (CASE WHEN "jira_customvalues.count_inquiry_type_child_1" IS NOT NULL THEN 0 ELSE 1 END) ELSE 2 END, CASE WHEN z__pivot_col_rank=1 THEN "jira_customvalues.count_inquiry_type_child_1" ELSE NULL END DESC, "jira_customvalues.count_inquiry_type_child_1" DESC, z__pivot_col_rank, "jira_issues.created_week") AS z___rank FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN "jira_customvalues.inquiry_type_parent_1" IS NULL THEN 1 ELSE 0 END, "jira_customvalues.inquiry_type_parent_1") AS z__pivot_col_rank FROM (
SELECT 
	DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK((jira_issues.created  AT TIME ZONE 'America/Los_Angeles')) % 7) - 1 + 7, 7)), (jira_issues.created  AT TIME ZONE 'America/Los_Angeles'))), '%Y-%m-%d') AS "jira_issues.created_week",
	jira_customvalues.Inquiry_Type_Parent  AS "jira_customvalues.inquiry_type_parent_1",
	COUNT(DISTINCT jira_customvalues.issue_id ) AS "jira_customvalues.count_inquiry_type_child_1"
FROM presto_looker_s3_operations.pdts2.LR_5WYQQJXM9D4ZJ72D68O2B_jira_issues AS jira_issues
LEFT JOIN presto_looker_s3_operations.pdts2.LR_5WY492XSL91CZWNVCRVJB_jira_customvalues AS jira_customvalues ON jira_issues.issue_id = jira_customvalues.issue_id 
LEFT JOIN presto_looker_s3_operations.s3.metapersons  AS metapersons ON (jira_customvalues.Person_ID = (cast(metapersons.person_id as bigint))
      and jira_issues.created >= (coalesce(metapersons.plan_start_date_dental, metapersons.plan_start_date_medical, metapersons.plan_start_date_pharmacy, metapersons.plan_start_date_vision))
      and jira_issues.created <= (coalesce(metapersons.plan_end_date_dental, metapersons.plan_end_date_medical, metapersons.plan_end_date_pharmacy, metapersons.plan_end_date_vision)))

WHERE ((jira_issues.created  >= CAST(CONCAT(DATE_FORMAT(TIMESTAMP '2016-01-01', '%Y-%m-%d %T '), 'America/Los_Angeles') AS TIMESTAMP))) AND (CASE
        WHEN jira_customvalues.Channel='Voicemail' OR jira_customvalues.Channel='Phone' THEN 'Phone'
        WHEN jira_customvalues.Channel='Member Portal' OR jira_customvalues.Channel='Email' THEN 'Email'
        WHEN jira_customvalues.Channel='Chat' THEN 'Chat'
        WHEN jira_customvalues.Channel='Ops' OR jira_customvalues.Channel='Mail' THEN 'Ops'
      END <> 'Ops' OR CASE
        WHEN jira_customvalues.Channel='Voicemail' OR jira_customvalues.Channel='Phone' THEN 'Phone'
        WHEN jira_customvalues.Channel='Member Portal' OR jira_customvalues.Channel='Email' THEN 'Email'
        WHEN jira_customvalues.Channel='Chat' THEN 'Chat'
        WHEN jira_customvalues.Channel='Ops' OR jira_customvalues.Channel='Mail' THEN 'Ops'
      END IS NULL) AND ((jira_customvalues.Inquiry_Type_Parent IS NOT NULL))
GROUP BY 1,2) ww
) bb WHERE z__pivot_col_rank <= 16384
) aa
) xx
) zz
 WHERE z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1 ORDER BY z___pivot_row_rank
