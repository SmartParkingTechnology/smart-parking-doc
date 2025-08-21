

WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'scmau' THEN 'Australia/Queensland' 
WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'scm' THEN 'Pacific/Auckland' 
WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'spukscvs' THEN 'Europe/London' 
WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'spGermanyManagedService' THEN 'Europe/Berlin' 
WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'spDenmarkManagedService' THEN 'Europe/Copenhagen' 
WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'cityOfMooneeValley' THEN 'Australia/Victoria' 
WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'spSwitzerlandManagedService' THEN 'Europe/Zurich'