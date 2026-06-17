csv | select id,name,score | filter "col(score) >= 10" | derive band=case_when(col(score)<50,'low','high') | csv
