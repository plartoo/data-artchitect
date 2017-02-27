SELECT 
'spot_id|week|ad_parent_no|ad_parent|advertiser_no|advertiser|ad_brand_no|ad_brand|ad_product_no|ad_product|ad_no|ad_copy|runtime_seconds|network_no|network|series_no|series|ad_time|ad_national_rtg';
select 
rentrak_spot_id
,rentrak_week
,rentrak_ad_parent_no
,rentrak_ad_parent
,rentrak_advertiser_no
,rentrak_advertiser
,rentrak_ad_brand_no
,rentrak_ad_brand
,rentrak_ad_product_no
,rentrak_ad_product
,rentrak_ad_no
,rentrak_ad_copy
,rentrak_runtime_seconds
,rentrak_network_no
,rentrak_network
,rentrak_series_no
,rentrak_series
,rentrak_ad_time
,rentrak_ad_national_rtg
from gaintheory_us_targetusa_14.incampaign_rentrak_spotid
where rentrak_week='35';






