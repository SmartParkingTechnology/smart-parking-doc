with site_name as (
select DISTINCT organization, parent, groupId, name as GroupName, actionType,timestamp, 
FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
where struct(key, timestamp) IN
(
select struct(key, max(timestamp)) FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService` 
where type='site'
group by key
)
and actionType !='GroupDeleted'
and organization =@orgId and parent = @regionId and groupId = @siteId
)

select c. *, s.GroupName  from (
    SELECT 
        DISTINCT 
        SPLIT(orsId, '#')[SAFE_OFFSET(1)] AS orsId,
        SPLIT(orsId, '#')[SAFE_OFFSET(2)] AS siteId,
        COALESCE(NULLIF(TRIM(inPlate), ''), NULLIF(TRIM(outPlate), '')) AS plate_no,
        DATETIME(updateTime, 'Europe/Copenhagen') AS update_time,
        COALESCE(NULLIF(TRIM(inVehicleColour), ''), NULLIF(TRIM(outVehicleColour), '')) AS vehicle_colour,
        COALESCE(NULLIF(TRIM(inVehicleMake), ''), NULLIF(TRIM(outVehicleMake), '')) AS vehicle_make,
        COALESCE(NULLIF(TRIM(inVehicleModel), ''), NULLIF(TRIM(outVehicleModel), '')) AS vehicle_model,
        --PARSE_DATE('%Y',
            COALESCE(NULLIF(TRIM(CAST(inVehicleYear AS STRING)), ''), 
            NULLIF(TRIM(CAST(outVehicleYear AS STRING)), '')) AS vehicle_year,
        COALESCE(NULLIF(TRIM(inVehicleFuelType), ''), NULLIF(TRIM(outVehicleFuelType), '')) AS vehicle_fueltype,
        COALESCE(inVehicleEmissionsCO2) AS vehicle_emissionCO2,
        COALESCE(inVehicleWeight, outVehicleWeight) AS vehicle_weight,
        CASE 
        WHEN arrivaltime IS NOT NULL AND departureTime IS NOT NULL 
        THEN ROUND((DATETIME_DIFF(DATETIME(departureTime,'Europe/Copenhagen'),DATETIME(arrivalTime,'Europe/Copenhagen'),SECOND))/60,1) 
        ELSE 0 END AS StayDurationMinutes,
        ROW_NUMBER() OVER (PARTITION BY orsId, inPlate, outPlate ORDER BY updateTime DESC) AS rn
    FROM `sc-neptune-production.smartcloud.lpr_matching_plates`
    WHERE date(arrivalTime) BETWEEN PARSE_DATE( '%Y%m%d',@startDate) and PARSE_DATE('%Y%m%d', @endDate)
    AND starts_with(orsId, 'spDenmarkManagedService#')
    AND SPLIT(orsId, '#')[SAFE_OFFSET(0)] = @orgId
    AND SPLIT(orsId, '#')[SAFE_OFFSET(1)] = @regionId
    AND SPLIT(orsId, '#')[SAFE_OFFSET(2)] = @siteId
) c
left join site_name s
on c.siteId = s.groupId
WHERE rn = 1
AND vehicle_model IS NOT NULL AND TRIM(vehicle_model) <> ''
AND vehicle_make IS NOT NULL AND TRIM(vehicle_make) <> ''
AND vehicle_fueltype IS NOT NULL AND TRIM(vehicle_fueltype) <> ''