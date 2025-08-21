SELECT  
      region,
      ST_GeogFromText(area) as area
    FROM
      bigquery-public-data.overture_maps.division_area
      where region like 'NZ%'