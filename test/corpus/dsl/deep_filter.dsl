csv | filter "((((((((col(score) + 1)))))))) > 2 and contains(col(name),'li')" | csv
