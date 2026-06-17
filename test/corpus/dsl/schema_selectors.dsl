csv | schema columns=id:int,score:number,name:string required=id non_null=score mode=warn | select starts_with(score)|id | csv
