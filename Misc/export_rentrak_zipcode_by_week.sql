SELECT 'spot_id|week|zip|ad_zip_rtg|ad_zip_aa';
select 
rentrak_spot_id
,rentrak_week
,rentrak_zip
,rentrak_ad_zip_rtg
,rentrak_ad_zip_aa
from gaintheory_us_targetusa_14.incampaign_rentrak_zipcode
where rentrak_week='27';
