grep -vE "^ROW*|^INSERT*|^FIELDS*|^STORED*|^CASE*|^from ${hiveconf:tab}|^order by uuid.|where*|and?uuid*|orderby*|WHEN*|Below*|use sh*|and uuid*" hive_export_query.txt | sed -E 's/.*as //g' |sed 's/set tab = //g'|sed 's/select //g'|sed 's/^set.*;//g' > sherlock_columns.txt


