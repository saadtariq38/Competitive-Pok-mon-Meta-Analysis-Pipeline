SELECT 
    t.id AS team_id,
    td.name AS trainer_name,
    td.rank AS trainer_rank,
    p1.name AS pokemon1_name,
    p2.name AS pokemon2_name,
    p3.name AS pokemon3_name,
    p4.name AS pokemon4_name,
    p5.name AS pokemon5_name,
    p6.name AS pokemon6_name,
    p1.win_ratio AS pokemon1_win_ratio,
    p2.win_ratio AS pokemon2_win_ratio,
    p3.win_ratio AS pokemon3_win_ratio,
    p4.win_ratio AS pokemon4_win_ratio,
    p5.win_ratio AS pokemon5_win_ratio,
    p6.win_ratio AS pokemon6_win_ratio,
    t.win_ratio AS team_win_ratio
FROM
    "AwsDataCatalog"."gluedatabase"."team_data_bucket"  t
JOIN
    "AwsDataCatalog"."gluedatabase"."trainercleaneddata"  td ON t.id = td.id
JOIN
    "AwsDataCatalog"."gluedatabase"."run_1714466883594_part_r_00000"  p1 ON t.p1 = p1.id
JOIN
    "AwsDataCatalog"."gluedatabase"."run_1714466883594_part_r_00000" p2 ON t.p2 = p2.id
JOIN
    "AwsDataCatalog"."gluedatabase"."run_1714466883594_part_r_00000" p3 ON t.p3 = p3.id
JOIN
    "AwsDataCatalog"."gluedatabase"."run_1714466883594_part_r_00000" p4 ON t.p4 = p4.id
JOIN
    "AwsDataCatalog"."gluedatabase"."run_1714466883594_part_r_00000" p5 ON t.p5 = p5.id
JOIN
    "AwsDataCatalog"."gluedatabase"."run_1714466883594_part_r_00000" p6 ON t.p6 = p6.id