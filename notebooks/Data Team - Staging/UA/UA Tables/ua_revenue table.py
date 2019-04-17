# Databricks notebook source
# MAGIC %run "Frequently Used/tp_functions"

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.io._
# MAGIC val path = dbutils.notebook.getContext().notebookPath.get
# MAGIC val pw = new PrintWriter(new File("path.txt" ))
# MAGIC pw.write(path)
# MAGIC pw.close

# COMMAND ----------

# MAGIC %python
# MAGIC environment = open('path.txt').read().split('/')[1]
# MAGIC 
# MAGIC if 'Staging' in environment:
# MAGIC   env = 'staging'
# MAGIC else:
# MAGIC   env = 'default'

# COMMAND ----------

## Additive 
day = 4

## Full data
# day = 600 

# COMMAND ----------

# DBTITLE 1,ad_cohort
ad_price = sql_data(r'''
select date, game, platform, country,
sum(reward_vid_count) as reward_vid_count, sum(inter_count) as inter_count, 
sum(reward_vid_count*rev_per_video_ad)/sum(reward_vid_count) as rev_per_video_ad, sum(inter_count*rev_per_inter)/sum(inter_count) as rev_per_inter, 
sum(video_rev_total) as video_rev_total, sum(inter_rev_total) as inter_rev_total
from

((
select date, case when lower(appname) like '%langu%' then 'languinis'
                    when lower(appname) like '%photo%' then 'photo finish'
                    when lower(appname) like '%tap%' then 'tap busters'
                    when lower(appname) like '%terra%' then 'terra genesis'
                    when lower(appname) like '%food truck%' then 'food truck chef'
                    when lower(appname) like '%operation%' then 'operation new earth'
                    when lower(appname) like '%horse%racing%manager%' then 'horse racing manager'
                    when lower(appname) like '%pocket%politics%' then 'pocket politics'
                    when lower(appname) like '%office%space%' then 'office space' 
                    when lower(appname) like '%nascar%' then 'nascar heat' 
                    when lower(appname) like '%two%dots%' then 'two dots' 
                    when lower(appname) like '%warhammer%' then 'warhammer' 
                    end as game,
case when lower(appname) like '%ios%' then 'ios' when lower(appname) like '%android%' then 'android' else appname end as platform, 
lower(countryCode) as country,
sum(case when lower(adunits) like '%rewarded%' then videocompletions end) as reward_vid_count,
sum(case when lower(adunits) like '%interstitial%' or lower(adunits) like '%offer%' then impressions end) as inter_count,
sum(case when lower(adunits) like '%rewarded%' then revenue end)/sum(case when lower(adunits) like '%rewarded%' then videocompletions end) as rev_per_video_ad,
sum(case when lower(adunits) like '%interstitial%' or lower(adunits) like '%offer%' then revenue end)/sum(case when lower(adunits) like '%interstitial%' or lower(adunits) like '%offer%' then impressions end) as rev_per_inter,
sum(case when lower(adunits) like '%rewarded%' then revenue end) as video_rev_total,
sum(case when lower(adunits) like '%interstitial%' or lower(adunits) like '%offer%' then revenue end) as inter_rev_total
from adnetworks.ironsource_publisher
where date>=date_add(current_date,-{days})
group by 1,2,3,4 having game is not null

union all
select date, case when lower(appname) like '%langu%' then 'languinis'
                    when lower(appname) like '%photo%' then 'photo finish'
                    when lower(appname) like '%tap%' then 'tap busters'
                    when lower(appname) like '%terra%' then 'terra genesis'
                    when lower(appname) like '%food truck%' then 'food truck chef'
                    when lower(appname) like '%operation%' then 'operation new earth'
                    when lower(appname) like '%horse%racing%manager%' then 'horse racing manager'
                    when lower(appname) like '%pocket%politics%' then 'pocket politics'
                    when lower(appname) like '%office%space%' then 'office space' 
                    when lower(appname) like '%nascar%' then 'nascar heat' 
                    when lower(appname) like '%two%dots%' then 'two dots' 
                    when lower(appname) like '%warhammer%' then 'warhammer' 
                    end as game,
case when lower(appname) like '%ios%' then 'ios' when lower(appname) like '%android%' then 'android' else appname end as platform, 
lower(countryCode) as country,
sum(case when lower(adunits) like '%rewarded%' then completions end) as reward_vid_count,
sum(case when lower(adunits) like '%interstitial%' or lower(adunits) like '%offer%' then impressions end) as inter_count,
sum(case when lower(adunits) like '%rewarded%' then revenue end)/sum(case when lower(adunits) like '%rewarded%' then completions end) as rev_per_video_ad,
sum(case when lower(adunits) like '%interstitial%' or lower(adunits) like '%offer%' then revenue end)/sum(case when lower(adunits) like '%interstitial%' or lower(adunits) like '%offer%' then impressions end) as rev_per_inter,
sum(case when lower(adunits) like '%rewarded%' then revenue end) as video_rev_total,
sum(case when lower(adunits) like '%interstitial%' or lower(adunits) like '%offer%' then revenue end) as inter_rev_total
from adnetworks.supersonic_publisher
where date<='2017-08-01'
group by 1,2,3,4 having game is not null
)

union all
(select date, case when lower(app_name) like '%almost%a%hero%' then 'almost a hero'
                   when lower(app_name) like '%dancing%' then 'dancing with the stars'
                   when lower(app_name) like '%nitro%nation%' then 'nitro nation' end as game, 
platform, country,
sum(impressions) as reward_vid_count, sum(0) as inter_count,
sum(revenue)/sum(impressions) as rev_per_video_ad, sum(0) as rev_per_inter,
sum(revenue) as video_rev_total, sum(0) as inter_rev_total
from adnetworks.tapdaq 
where date>=date_add(current_date,-{days})
group by 1,2,3,4 having game is not null)

union all
(select date, case when lower(name) like '%photo%finish%' then 'photo finish'
                   when lower(name) like 'food%truck%chef%' then 'food truck chef' end as game,
case when lower(platform)='android' then 'android' else 'ios' end as platform, country,
sum(impressions) as reward_vid_count, sum(0) as inter_count,
sum(revenue)/sum(impressions) as rev_per_video_ad, sum(0) as rev_per_inter,
sum(revenue) as video_rev_total, sum(0) as inter_rev_total
from adnetworks.tapjoy_publisher 
where date>=date_add(current_date,-{days}) and lower(country)!='global'
group by 1,2,3,4 having game is not null))
group by 1,2,3,4
'''.format(days=day),'ad_price',1) 


ad_watch = sql_data(r'''
with install_time as (
select adjust_id, time, install_time, network, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, 'ad_completed' as evtname_sub, 'rewarded video' as ad_type, platform, country, 'photo finish' as game
from photofinish.adjust
where evtname='ad_event' and (evtname_sub='ad_completed' or evtname_sub is null) and event_date>=date_add(current_date,-{days})

union all
select adjust_id, time, install_time, network, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, 'ad_completed' as evtname_sub, ad_type, platform, country, 'languinis' as game
from languinis.adjust
where evtname='ad_event' and evtname_sub='ad_completed' and event_date>=date_add(current_date,-{days})

union all
select adjust_id, time, install_time, network, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, 'ad_completed' as evtname_sub, 'rewarded video' as ad_type, platform, country, 'tap busters' as game
from tapbusters.adjust
where evtname='ad_event' and evtname_sub='ad_completed' and event_date>=date_add(current_date,-{days})

union all
select adjust_id, time, install_time, network, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, 'ad_completed' as evtname_sub, 'rewarded video' as ad_type, platform, country, 'terra genesis' as game
from terragenesis.adjust
where evtname='ad_event' and (evtname_sub='ad_completed' or evtname_sub is null) and event_date>=date_add(current_date,-{days}) and date(install_time)>='2017-11-17'

union all
select adjust_id, time, install_time, network, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, 'ad_completed' as evtname_sub, 'rewarded video' as ad_type, platform, country, 'food truck chef' as game
from foodtruckchef.adjust
where evtname='ad_event' and evtname_sub='ad_completed' and event_date>=date_add(current_date,-{days}) and date(install_time)>='2018-02-12'

union all
select adjust_id, time, install_time, network, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, 'ad_completed' as evtname_sub, 'rewarded video' as ad_type, platform, country, 'horse racing manager' as game
from photofinish2.adjust
where evtname='ad_event' and (evtname_sub='ad_completed' or evtname_sub is null) and event_date>=date_add(current_date,-{days})

/*union all
select adid as adjust_id, time, install_time, network, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, 'ad_completed' as evtname_sub, 'rewarded video' as ad_type, platform, country, 'office space' as game
from ofs_adjust
where evtname='offerwall' and event_date>=date_add(current_date,-{days}) and date(install_time)>='2018-07-14' and event_date>='2018-07-14'*/

/*union all
select adid as adjust_id, time, install_time, network, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, 'ad_completed' as evtname_sub, 'rewarded video' as ad_type, platform, country, 'pocket politics' as game
from pkp_adjust
where evtname='offerwall' and event_date>=date_add(current_date,-{days}) and date(install_time)>='2018-07-14' and event_date>='2018-07-14'*/

union all
select adjust_id, time, install_time, network, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, 'ad_completed' as evtname_sub, ad_type, platform, country, 'nascar heat' as game
from nascarheat.adjust
where evtname='ad_event' and evtname_sub='ad_completed' and event_date>=date_add(current_date,-{days}) and date(install_time)>='2018-07-25' and (lower(tracker_name) like '%tiltingpoint%' or lower(tracker_name)='organic')

union all
select adjust_id, time, 
min(install_time) over (partition by user_id order by user_id) as install_time, 
network, campaign, adgroup, creative, evtname_sub, ad_type, platform, country, game
from
(select adjust_id, time, install_time, network, 
split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, 'ad_completed' as evtname_sub, 'rewarded video' as ad_type, platform, country, 'almost a hero' as game, coalesce(idfa, idfv, aifa, andi) as user_id 
from almostahero.adjust 
where evtname='ad_event' and evtname_sub='ad_completed' and date(time)>=date_add(current_date,-{days}) and date(time)>'2018-07-01'
union all 
select appsflyerid as adjust_id, eventtime as time, installtime as install_time, mediasource as network,
concat(coalesce(campaign,'null'),':',coalesce(campaignid,'null')) as campaign, case when lower(mediasource) like '%applovin%' then concat(coalesce(siteid,'null'),':',coalesce(subsiteid,'null')) else concat(coalesce(adset,siteid,'null'),':',coalesce(adsetid,subsiteid,'null')) end as adgroup, concat(coalesce(ad,'null'),':',coalesce(adid,'null')) as creative,
'ad_completed' as evtname_sub, 'rewarded video' as ad_type, platform, case when countrycode='UK' then 'gb' else lower(countrycode) end as country, 'almost a hero' as game, coalesce(idfa, idfv, AdvertisingID, AndroidID) as user_id 
from almostahero.appsflyer 
where eventname='ad_watched' and date(eventtime)>=date_add(current_date,-{days}) and date(eventtime)<='2018-07-01') 

union all
select adjust_id, time, install_time, network, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, 'ad_completed' as evtname_sub, 'rewarded video' as ad_type, platform, country, 'two dots' as game
from twodots.adjust
where evtname='watchedad' and (lower(tracker_name) like '%tiltingpoint%' or lower(tracker_name)='organic') and event_date>=date_add(current_date,-{days}) and date(install_time)>='2018-08-20'

union all 
select adjust_id, time, install_time, network, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, 'ad_completed' as evtname_sub, 'rewarded video' as ad_type, platform, country, 'warhammer' as game
from warhammer.adjust
where evtname='ad_event' and evtname_sub='ad_completed' and event_date>=date_add(current_date,-{days}) and date(install_time)>= '2018-10-27'

union all
select appsflyerid as adjust_id, eventtime as time, installtime as install_time, mediasource as network,
concat(coalesce(campaign,'null'),':',coalesce(campaignid,'null')) as campaign, case when lower(mediasource) like '%applovin%' then concat(coalesce(siteid,'null'),':',coalesce(subsiteid,'null')) else concat(coalesce(adset,siteid,'null'),':',coalesce(adsetid,subsiteid,'null')) end as adgroup, concat(coalesce(ad,'null'),':',coalesce(adid,'null')) as creative,
'ad_complete' as evtname_sub, 'rewarded video' as ad_type, platform, case when countrycode='UK' then 'gb' else lower(countrycode) end as country, 'dancing with the stars' as game
from dancingwiththestars.appsflyer
where eventname='ad_event' and date(eventtime)>=date_add(current_date,-{days}) and date(eventtime)<=current_date
and (mediasource is null or lower(campaign) like '%tiltingpoint%')

union all
select appsflyerid as adjust_id, eventtime as time, installtime as install_time, mediasource as network,
concat(coalesce(campaign,'null'),':',coalesce(campaignid,'null')) as campaign
, case when lower(mediasource) like '%applovin%' then concat(coalesce(siteid,'null'),':',coalesce(subsiteid,'null')) else concat(coalesce(adset,siteid,'null'),':',coalesce(adsetid,subsiteid,'null')) end as adgroup
, concat(coalesce(ad,'null'),':',coalesce(adid,'null')) as creative,
'ad_complete' as evtname_sub, 'rewarded video' as ad_type, platform, case when countrycode='UK' then 'gb' else lower(countrycode) end as country, 'nitro nation' as game
from nitronation.appsflyer
where eventname='ad_event' and date(eventtime)>=date_add(current_date,-{days}) and date(eventtime)<=current_date
and (mediasource is null or lower(campaign) like '%tiltingpoint%')
)

select game, country, platform, coalesce(network,'Organic') as network,
case when lower(network) like '%applovin%' then split(campaign, ':')[0]
     when lower(network) like '%ironsource%' then replace(campaign, concat('_',substring_index(campaign,'_',-1)))
     when lower(split(split(campaign,' \\(')[0],':')[0]) like '%adquant_%' then substring(split(split(campaign,' \\(')[0],':')[0],9,200)
     else coalesce(split(split(campaign,' \\(')[0],':')[0],'null')
     end as campaign_name,
case when lower(network) like '%applovin%' then coalesce(split(campaign, ':')[1],'null') when lower(network) like  '%ironsource%' then substring_index(campaign,'_',-1) else coalesce(split(split(campaign,' \\(')[1],'\\)')[0], split(campaign,':')[1], 'null') end as campaign_id,
case when lower(network) like '%google universal%' or lower(network) like '%adwords%' then country
     when lower(network) like '%ironsource%' or lower(network) like '%applovin%' then 'null'
     when lower(network) like '%apple%' then concat(split(split(adgroup,':')[0],' \\(')[0],'_',coalesce(split(creative,':')[0],'null'))
     when lower(network) like '%liftoff%' then coalesce(split(adgroup,'_')[0],'null')                                                                                 
     else coalesce(split(split(adgroup,':')[0],' \\(')[0],'null') end as adset_name,
case when lower(network) like '%google universal%' or lower(network) like '%adwords%' then 'null'
     when lower(network) like '%applovin%' then coalesce(split(adgroup,':')[0],'null')
     when lower(network) like '%ironsource%' then concat(split(adgroup,'_')[0],'_') --,concat(split(split(adgroup,'\\:')[0],'_')[0],'_'))
     else coalesce(split(split(adgroup,' \\(')[1],'\\)')[0],split(adgroup,'\\:')[1],split(adgroup,'_')[1],'null') end as adset_id, 
date(install_time) as install_date,
greatest(floor((unix_timestamp(time)-unix_timestamp(install_time))/3600/24),0) as days_installed, 
date_add(date(install_time),greatest(floor((unix_timestamp(time)-unix_timestamp(install_time))/3600/24),0)) as date,
count(case when lower(ad_type) like '%reward%' then 'Rewarded Video' end) as reward_vid_count,
count(case when lower(ad_type) like '%inter%' then 'Interstitial' end) as inter_count,
count(distinct adjust_id) as user_count,
count(1) as ad_views
from install_time
group by 1,2,3,4,5,6,7,8,9,10,11


'''.format(days=day),'ad_watch',1)

ad_cohort = sql_data(r'''
with raw as(
select a.*, a.reward_vid_count * rev_per_video_ad as video_rev, a.inter_count * rev_per_inter as inter_rev,
       (ifnull(a.reward_vid_count * rev_per_video_ad, 0)+ifnull(a.inter_count * rev_per_inter, 0)) as total_ad_rev
from ad_watch a left join ad_price b 
on lower(ifnull(a.date,'null'))=lower(ifnull(b.date,'null')) and lower(ifnull(a.platform,'null'))=lower(ifnull(b.platform,'null')) 
and lower(ifnull(a.game,'null'))=lower(ifnull(b.game,'null')) and lower(ifnull(a.country,'null'))=lower(ifnull(b.country,'null')))

select game, platform, country,
case when lower(network) like '%adcolony%' then 'adcolony'
     when lower(network) like '%unity%' then 'unity ads'
     when lower(network) like '%apple%' then 'apple search ads'
     when (lower(network) like '%google universal%' or lower(network) like '%adwords%') then 'google adwords'
     when lower(network) like '%ironsource%' then 'ironsource'
     when lower(network) like '%applovin%' then 'applovin'
     when lower(network) like '%vungle%' then 'vungle'
     when (lower(network)='organic' or lower(network)='google organic search') then 'organic'
     when (lower(network) like '%facebook%' or lower(network) like '%instagram%') then 'facebook' 
     when lower(network) like '%liftoff%' then 'liftoff'
     when lower(network) like '%snapchat%' then 'snapchat'   
     when lower(network) like '%digital%turbine%' then 'digital turbine'
     when lower(network) like '%tapjoy%' then 'tapjoy'
     when lower(network) like '%twitter%' then 'twitter'
     when lower(network) like '%taptica%' then 'taptica'
     when lower(network) like '%crossinstall%' then 'crossinstall'
     when lower(network) like '%smadex%' then 'smadex'
     else 'others' end as network,
campaign_name, campaign_id, adset_name, adset_id,
install_date, days_installed,
sum(reward_vid_count) as reward_vid_count, sum(inter_count) as inter_count, sum(user_count) as user_count, sum(ad_views) as ad_views,
sum(video_rev) as video_rev, sum(inter_rev) as inter_rev, sum(total_ad_rev) as total_ad_rev
from raw
group by 1,2,3,4,5,6,7,8,9,10

union all (select * from {environment}.ds_lkt_ad_cohort where not (install_date >= '2019-03-14' and lower(network) like '%applovin%'))

'''.format(environment=env),'ad_cohort',1)

# COMMAND ----------

display(ad_watch)

# COMMAND ----------

sqlContext.table('ad_cohort').repartition(20).write.format('delta').option("overwriteSchema", "true").mode("overwrite").saveAsTable("{environment}.ad_cohort_backup".format(environment=env))

# COMMAND ----------

# DBTITLE 1,iap_cohort
iap_cohort = sql_data(r'''
with combine as(
select 'photo finish' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from photofinish.adjust where event_date>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')
union all
select 'languinis' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from languinis.adjust where event_date>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')
union all
select 'siege' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from siege.adjust where event_date>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')
union all
select  'tap busters' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from tapbusters.adjust where event_date>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')
union all
select 'terra genesis' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from terragenesis.adjust where event_date>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%') and date(install_time)>='2017-11-17'
union all
select  'operation new earth' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from newearth.adjust where event_date>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')
/*union all
select 'diesel drag racing' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from dd_adjust where event_date>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')*/
union all
select 'food truck chef' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from foodtruckchef.adjust where event_date>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%') and date(install_time)>='2018-02-12'
union all
select 'horse racing manager' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from photofinish2.adjust where event_date>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')
union all
select 'the arcana' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from thearcana.adjust where event_date>=date_add(current_date,-{days}) and date(install_time)>='2018-03-10'
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')

union all
select 'nascar heat' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from nascarheat.adjust where event_date>=date_add(current_date,-{days}) and date(install_time)>='2018-07-25'
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')
and (lower(tracker_name) like '%tiltingpoint%' or lower(tracker_name)='organic')

union all 
select game, network, platform, country, campaign, adgroup, creative, 
min(install_time) over (partition by user_id order by user_id) as install_time, 
time, adjust_id, transaction_id, amount
from
(select'almost a hero' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount, coalesce(idfa, idfv, aifa, andi) as user_id 
from almostahero.adjust  where date(time)>=date_add(current_date,-{days}) and date(time)>='2018-07-01'
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')
union all
select 'almost a hero' as game, coalesce(mediasource,'Organic') as network, platform, case when countrycode='UK' then 'gb' else lower(countrycode) end as country, 
concat(coalesce(campaign,'null'),':',coalesce(campaignid,'null')) as campaign, case when lower(mediasource) like '%applovin%' then concat(coalesce(siteid,'null'),':',coalesce(subsiteid,'null')) else concat(coalesce(adset,siteid,'null'),':',coalesce(adsetid,subsiteid,'null')) end as adgroup, concat(coalesce(ad,'null'),':',coalesce(adid,'null')) as creative, installtime as install_time, eventtime as time, appsflyerid as adjust_id, null as transaction_id, eventrevenueusd as amount, coalesce(idfa, idfv, AdvertisingID, AndroidID) as user_id 
from almostahero.appsflyer  
where date(eventtime)>=date_add(current_date,-{days}) and eventname in ('af_purchase', 'install') and date(eventtime)<='2018-07-01')

union all 
select 'two dots' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from twodots.adjust where date(install_time)>='2018-08-20' and date(install_time)>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased','purchasemade') or evtname is null or evtname like '%subscription%')  and (lower(tracker_name) like '%tiltingpoint%' or lower(tracker_name)='organic') 

union all
select 'pacific rim', network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from pacificrim.adjust where event_date>=date_add(current_date,-{days}) and date(install_time)>='2018-08-18'
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')
and (lower(tracker_name) like '%tilting%' or lower(tracker_name)='organic'  or lower(tracker_name) like '%uac%')

union all 
select 'dragon rpg' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from dragonrpg.adjust where date(install_time)>='2018-09-25' and date(install_time)>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased','purchasemade') or evtname is null or evtname like '%subscription%')  and (lower(tracker_name) like '%tiltingpoint%' or lower(tracker_name)='organic') 

union all
select 'warhammer' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from warhammer.adjust where date(install_time)>='2018-10-27' and date(install_time)>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')

union all
select 'mutants genetic gladiators' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative, install_time, time, adjust_id, transaction_id, amount
from mutantgeneticgladiators.adjust where date(install_time)>='2019-03-15' and date(install_time)>=date_add(current_date,-{days})
and (evtname in ('session','app_launch','install','buy_iap','iap_buy','reattribution','Purchased') or evtname is null or evtname like '%subscription%')


union all
select 'dancing with the stars' as game, coalesce(mediasource,'Organic') as network, platform, case when countrycode='UK' then 'gb' else lower(countrycode) end as country, concat(coalesce(campaign,'null'),':',coalesce(campaignid,'null')) as campaign, case when lower(mediasource) like '%applovin%' then concat(coalesce(siteid,'null'),':',coalesce(subsiteid,'null')) else concat(coalesce(adset,siteid,'null'),':',coalesce(adsetid,subsiteid,'null')) end as adgroup, concat(coalesce(ad,'null'),':',coalesce(adid,'null')) as creative, installtime as install_time, eventtime as time, appsflyerid as adjust_id, null as transaction_id, eventrevenueusd as amount
from dancingwiththestars.appsflyer where date(eventtime)>=date_add(current_date,-{days}) and eventname in ('af_purchase', 'install') and date(eventtime)<=current_date
and (mediasource is null or lower(campaign) like '%tiltingpoint%')

union all
select 'star trek timelines' as game, coalesce(mediasource,'Organic') as network, platform, 
case when countrycode='UK' then 'gb' else lower(countrycode) end as country, 
concat(coalesce(campaign,'null'),':',coalesce(campaignid,'null')) as campaign, 
case when lower(mediasource) like '%applovin%' then concat(coalesce(siteid,'null'),':',coalesce(subsiteid,'null')) 
     when lower(mediasource) like '%unity%' or lower(mediasource) like '%tapjoy%' or lower(mediasource) like '%adcolony%' 
     then concat(coalesce(adset,'null'), ':', coalesce(siteid,'null'))
     else concat(coalesce(adset,siteid,'null'),':',coalesce(adsetid,subsiteid,siteid,'null')) end as adgroup,
concat(coalesce(ad,'null'),':',coalesce(adid,'null')) as creative, installtime as install_time, eventtime as time, appsflyerid as adjust_id, null as transaction_id, eventrevenueusd as amount
from startrektimelines.appsflyer where date(eventtime)>=date_add(current_date,-{days}) and eventname in ('af_purchase', 'install') and date(eventtime)<=current_date 
and date(installtime) >= '2018-06-30'

union all
select 'nitro nation' as game, coalesce(mediasource,'Organic') as network, platform, case when countrycode='UK' then 'gb' else lower(countrycode) end as country, 
concat(coalesce(campaign,'null'),':',coalesce(campaignid,'null')) as campaign, case when lower(mediasource) like '%applovin%' then concat(coalesce(siteid,'null'),':',coalesce(subsiteid,'null')) else concat(coalesce(adset,siteid,'null'),':',coalesce(adsetid,subsiteid,'null')) end as adgroup, concat(coalesce(ad,'null'),':',coalesce(adid,'null')) as creative, installtime as install_time, eventtime as time, appsflyerid as adjust_id, null as transaction_id, eventrevenueusd as amount
from nitronation.appsflyer where date(eventtime)>=date_add(current_date,-{days}) and eventname in ('af_purchase', 'install') and date(eventtime)<=current_date
and (mediasource is null or lower(campaign) like '%tiltingpoint%')

union all
select 'office space' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative,  install_time, time, adjust_id, null as transaction_id, amount
from ofs_adjust where (event_date>=date_add(current_date,-{days}) and date(install_time)>='2018-07-14') and (lower(tracker_name) like '%tiltingpoint%' or lower(tracker_name)='organic')

union all
select 'pocket politics' as game, network, platform, country, split(tracker_name,'::')[1] as campaign, split(tracker_name,'::')[2] as adgroup, split(tracker_name,'::')[3] as creative,  install_time, time, adjust_id, null as transaction_id, amount
from pkp_adjust where (event_date>=date_add(current_date,-{days}) and date(install_time)>='2018-07-14') and (lower(tracker_name) like '%tiltingpoint%' or lower(tracker_name)='organic')

union all
select 'jackpot empire slots' as game, coalesce(mediasource,'Organic') as network, platform, 
case when countrycode='UK' then 'gb' else lower(countrycode) end as country, 
concat(coalesce(campaign,'null'),':',coalesce(campaignid,'null')) as campaign, 
case when lower(mediasource) like '%applovin%' then concat(coalesce(siteid,'null'),':',coalesce(subsiteid,'null')) 
     when lower(mediasource) like '%unity%' or lower(mediasource) like '%tapjoy%' or lower(mediasource) like '%adcolony%' 
     then concat(coalesce(adset,'null'), ':', coalesce(siteid,'null'))
     else concat(coalesce(adset,siteid,'null'),':',coalesce(adsetid,subsiteid,siteid,'null')) end as adgroup,
concat(coalesce(ad,'null'),':',coalesce(adid,'null')) as creative, installtime as install_time, eventtime as time, appsflyerid as adjust_id, null as transaction_id, eventrevenueusd as amount
from jackpotempireslots.appsflyer where date(eventtime)>=date_add(current_date,-{days}) and eventname in ('af_purchase', 'install') 
and date(eventtime)<=current_date 
and date(installtime) >= '2018-12-21'


union all
select 'toy party' as game, coalesce(mediasource,'Organic') as network, platform, 
case when countrycode='UK' then 'gb' else lower(countrycode) end as country, 
concat(coalesce(campaign,'null'),':',coalesce(campaignid,'null')) as campaign, 
case when lower(mediasource) like '%applovin%' then concat(coalesce(siteid,'null'),':',coalesce(subsiteid,'null')) 
     when lower(mediasource) like '%unity%' or lower(mediasource) like '%tapjoy%' or lower(mediasource) like '%adcolony%' 
     then concat(coalesce(adset,'null'), ':', coalesce(siteid,'null'))
     else concat(coalesce(adset,siteid,'null'),':',coalesce(adsetid,subsiteid,siteid,'null')) end as adgroup,
concat(coalesce(ad,'null'),':',coalesce(adid,'null')) as creative, installtime as install_time, eventtime as time, appsflyerid as adjust_id, null as transaction_id, eventrevenueusd as amount
from toyparty.appsflyer where date(eventtime)>=date_add(current_date,-{days}) and eventname in ('af_purchase', 'install') 
and date(eventtime)<=current_date 
and date(installtime) >= '2019-01-15'

)


select 
game,            
case when lower(network) like '%apple%' then 'apple search ads'
     when (lower(network) like '%google universal%' or lower(network) like '%adwords%' or lower(network)='google (unknown)') then 'google adwords'
     when lower(network) like '%ironsource%' then 'ironsource'
     when lower(network) like '%applovin%' then 'applovin'
     when (lower(network) like '%facebook%' or lower(network) like '%instagram%') then 'facebook'
     when lower(network) like '%adcolony%' then 'adcolony'
     when lower(network) like '%unity%' then 'unity ads'
     when lower(network) like '%vungle%' then 'vungle'
     when (lower(network)='organic' or lower(network)='google organic search') then 'organic' 
     when lower(network) like '%liftoff%' then 'liftoff'
     when lower(network) like '%snapchat%' then 'snapchat'
     when lower(network) like '%digital%turbine%' then 'digital turbine'
     when lower(network) like '%tapjoy%' then 'tapjoy'
     when lower(network) like '%twitter%' then 'twitter'
     when lower(network) like '%crossinstall%' then 'crossinstall'
     when lower(network) like '%taptica%' then 'taptica'
     when lower(network) like '%smadex%' then 'smadex'
     else 'others' end as network,
platform, country,
case when lower(network) like '%applovin%' then split(campaign, ':')[0]
     when lower(network) like  '%ironsource%' then replace(campaign, concat('_',substring_index(campaign,'_',-1)))
     when lower(split(split(campaign,' \\(')[0],':')[0]) like '%adquant_%' then substring(split(split(campaign,' \\(')[0],':')[0],9,200)
     else coalesce(split(split(campaign,' \\(')[0],':')[0],'null')
     end as campaign_name,
case when lower(network) like '%applovin%' then coalesce(split(campaign, ':')[1],'null') when lower(network) like  '%ironsource%' then substring_index(campaign,'_',-1) else coalesce(split(split(campaign,' \\(')[1],'\\)')[0], split(campaign,':')[1], 'null') end as campaign_id,
case when lower(network) like '%google universal%' or lower(network) like '%adwords%' then country
     when lower(network) like '%ironsource%' or lower(network) like '%applovin%' then 'null'
     when lower(network) like '%apple%' then concat(split(split(adgroup,':')[0],' \\(')[0],'_',coalesce(split(creative,':')[0],'null'))
     when lower(network) like '%liftoff%' then coalesce(split(adgroup,'_')[0],'null') 
     when lower(network) like '%unity%' or lower(network) like '%tapjoy%' or lower(network) like '%adcolony%' then split(adgroup,':')[0] 
     else coalesce(split(split(adgroup,':')[0],' \\(')[0],'null') end as adset_name,
case when lower(network) like '%google universal%' or lower(network) like '%adwords%' then 'null'
     when lower(network) like '%applovin%' then coalesce(split(adgroup,':')[0],'null')
     when lower(network) like '%ironsource%' then concat(split(adgroup,'_')[0],'_') --,concat(split(split(adgroup,'\\:')[0],'_')[0],'_'))
     when lower(network) like '%unity%' or lower(network) like '%tapjoy%' or lower(network) like '%adcolony%' then split(adgroup,':')[1]
     else coalesce(split(split(adgroup,' \\(')[1],'\\)')[0],split(adgroup,'\\:')[1],split(adgroup,'_')[1],'null') end as adset_id, 
date(install_time) as install_date,
greatest(floor((unix_timestamp(time)-unix_timestamp(install_time))/3600/24),0) as days_installed,
count(distinct adjust_id) as user_count,
count(distinct case when amount>0 then adjust_id end) as payer_count,
count(case when amount>0 then adjust_id end) as purchase, sum(amount*0.69) as net_revenue
from combine

group by 1,2,3,4,5,6,7,8,9,10 order by 1,2,3,4,5,6,7,8,9,10

'''.format(days=day),'iap_cohort',1)

# COMMAND ----------

sqlContext.table('iap_cohort').repartition(20).write.format('delta').option("overwriteSchema", "true").mode("overwrite").saveAsTable("{environment}.iap_cohort_backup".format(environment=env))

# COMMAND ----------

# DBTITLE 1,ua_revenue
ua_revenue = sql_data(r'''
select 
coalesce(b.game,a.game) as game, coalesce(b.platform,a.platform) as platform, coalesce(b.country,a.country) as country, coalesce(b.network,a.network) as network, coalesce(b.campaign_name,a.campaign_name) as campaign_name, coalesce(b.campaign_id,a.campaign_id) as campaign_id, 
coalesce(b.adset_name,a.adset_name) as adset_name, coalesce(b.adset_id,a.adset_id) as adset_id, 
coalesce(b.install_date,a.install_date) as install_date, coalesce(b.days_installed,a.days_installed) as days_installed, 
coalesce(b.user_count,a.user_count) as cohort_size, coalesce(a.user_count,0) as ad_watcher,coalesce(a.ad_views,0) as ad_views, coalesce(b.purchase,0) as purchase, coalesce(b.payer_count,0) as payer, 
coalesce(b.net_revenue,0) as net_revenue, coalesce(a.total_ad_rev,0) as total_ad_rev
from ad_cohort a full join iap_cohort b
on a.game=b.game and a.platform=b.platform and a.country=b.country and a.network=b.network
and a.campaign_name=b.campaign_name and a.campaign_id=b.campaign_id and a.adset_name=b.adset_name and a.adset_id=b.adset_id
and a.install_date=b.install_date and a.days_installed=b.days_installed
where coalesce(b.install_date,a.install_date)<current_date 
union all
select * from datascience.thv_revenue_prep
''', 'ua_revenue',1)

# COMMAND ----------

if day>=400:
  print('backfill historical')
  sql_data('''
  select a.game,a.platform,a.country,a.network,a.campaign_name,a.campaign_id,a.adset_name,a.adset_id,a.install_date, a.days_installed, datediff(current_date, a.install_date) as days_existed,
  a.purchase,a.payer, a.ad_views, a.ad_watcher,a.net_revenue,a.total_ad_rev,b.total_purchase,b.cohort_size
  from
  (select game,platform,country,network,campaign_name,campaign_id,adset_name,adset_id,
  install_date,days_installed,purchase,payer, ad_views, ad_watcher, net_revenue,total_ad_rev from ua_revenue) a full join
  (select game,platform,country,network,campaign_name,campaign_id,adset_name,adset_id,install_date,
  max(cohort_size) as cohort_size, sum(purchase) as total_purchase
  from ua_revenue group by 1,2,3,4,5,6,7,8,9) b
  on a.game=b.game and a.platform=b.platform and a.country=b.country and a.network=b.network
  and a.campaign_name=b.campaign_name and a.campaign_id=b.campaign_id and a.adset_name=b.adset_name and a.adset_id=b.adset_id
  and a.install_date=b.install_date
  where cohort_size>0 and days_installed>=0 
  ''','ua_revenue_full',0).repartition(20).write.mode("overwrite").format('delta').option("overwriteSchema", "true").saveAsTable("{environment}.ds_summary_ua_revenue".format(environment=env))

else:
  ua_revenue_add = sql_data(r'''
  with combine as (
  select game,platform,country,network,campaign_name,campaign_id,adset_name,adset_id,
  install_date, days_installed, 
  max(cohort_size) as cohort_size, max(purchase) as purchase, max(payer) as payer, max(ad_views) as ad_views, max(ad_watcher) as ad_watcher, 
  max(net_revenue) as net_revenue, max(total_ad_rev) as total_ad_rev
  from
  (select * from ua_revenue 
  union all 
  select game,platform,country,network,campaign_name,campaign_id,adset_name,adset_id,
  install_date,days_installed,cohort_size,ad_watcher,ad_views,purchase,payer,net_revenue,total_ad_rev 
  from {environment}.ds_summary_ua_revenue)
  group by 1,2,3,4,5,6,7,8,9,10)

  select 
  a.game,a.platform,a.country,a.network,a.campaign_name,a.campaign_id,a.adset_name,a.adset_id,
  a.install_date, a.days_installed, datediff(current_date, a.install_date) as days_existed,
  a.purchase,a.payer, a.ad_views, a.ad_watcher,a.net_revenue,a.total_ad_rev,b.total_purchase,b.cohort_size
  from
  (select game,platform,country,network,campaign_name,campaign_id,adset_name,adset_id,
  install_date,days_installed,purchase,payer, ad_views, ad_watcher, net_revenue,total_ad_rev from combine) a full join
  (select game,platform,country,network,campaign_name,campaign_id,adset_name,adset_id,install_date,
  max(cohort_size) as cohort_size, sum(purchase) as total_purchase
  from combine group by 1,2,3,4,5,6,7,8,9) b
  on a.game=b.game and a.platform=b.platform and a.country=b.country and a.network=b.network
  and a.campaign_name=b.campaign_name and a.campaign_id=b.campaign_id and a.adset_name=b.adset_name and a.adset_id=b.adset_id
  and a.install_date=b.install_date
  where cohort_size>0 and days_installed>=0 
  '''.format(environment=env),'ua_revenue_add',1)

# COMMAND ----------

if day<400:
  sqlContext.table('ua_revenue_add').repartition(20).write.format('delta').option("overwriteSchema", "true").mode("overwrite").saveAsTable("{environment}.ds_summary_ua_revenue_add".format(environment=env))
  temp = sqlContext.table("{environment}.ds_summary_ua_revenue_add".format(environment=env))
  temp.repartition(20).write.mode("overwrite").format('delta').option("overwriteSchema", "true").saveAsTable("{environment}.ds_summary_ua_revenue".format(environment=env))

# COMMAND ----------

