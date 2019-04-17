# Databricks notebook source
# MAGIC %run "Frequently Used/tp_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC clear cache

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
# MAGIC if 'Production' in environment:
# MAGIC   env = 'default'
# MAGIC else:
# MAGIC   env = 'staging'
# MAGIC   
# MAGIC print(env)  

# COMMAND ----------

# DBTITLE 0,Spend
ua_spend = sql_data(r'''
with spend_data as(
select 'facebook' as network, campaign_id, 
case when lower(campaign_name) like '%adquant_%' then substring(campaign_name,9,200)
     else campaign_name end as campaign_name,
     adset_id, adset_name, ad_id, ad_name, account_name,
lower(country) as country, case when lower(campaign_name) like '%android%' then 'android' else 'ios' end as platform, date_start as install_date, 
sum(impressions) as impressions, sum(clicks) as clicks, sum(spend) as spend, sum(mobile_app_install) as installs
from adnetworks.facebook_ads_country 
where lower(account_name) not like '%test%'
group by 1,2,3,4,5,6,7,8,9,10,11
union all
select 'adcolony' as network, campaign_id, campaign_name, null as adset_id, null as adset_name, null as ad_id, null as ad_name, null as account_name,
lower(country) as country, case when lower(campaign_name) like '%android%' then 'android' else 'ios' end as platform, date as install_date,
sum(impressions) as impressions, sum(total_clicks) as clicks, sum(spend) as spend, sum(installs) as installs
from adnetworks.adcolony_advertiser 
group by 1,2,3,4,5,6,7,8,9,10,11
union all
select 'vungle' as network, campaign_id, campaign_name, null as adset_id, null as adset_name, creative_id as ad_id, creative_name as ad_name, null as account_name,
lower(country) as country, lower(platform) as platform, date as install_date,
sum(views) as impressions, sum(clicks) as clicks, sum(spend) as spend, sum(installs) as installs
from adnetworks.vungle_advertiser
group by 1,2,3,4,5,6,7,8,9,10,11
union all
select 'applovin' as network, null as campaign_id, campaign as campaign_name, app_id_external as adset_id, null as adset_name, null as ad_id, ad as ad_name, null as account_name,
lower(country) as country, lower(platform) as platform, day as install_date,
sum(impressions) as impressions, sum(clicks) as clicks, sum(cost) as spend, sum(conversions) as installs
from adnetworks.applovin_advertiser where not (day >= '2019-03-14' and (lower(campaign) like '%lkt%' or lower(campaign) like '%lucktastic%'))
group by 1,2,3,4,5,6,7,8,9,10,11
union all
select 'apple search ads' as network, campaign_id, 
case when lower(campaign_name) like '%adquant_%' then substring(campaign_name,9,200)
     else campaign_name end as campaign_name,
null as adset_id, adgroupname as adset_name, null as ad_id, keyword as ad_name, null as account_name,
lower(case when split(campaign_name,'-')[2] is not null and length(split(campaign_name,'-')[2])=2 then split(campaign_name,'-')[2] else split(campaign_name,'-')[1] end) as country, case when lower(campaign_name) like '%android%' then 'android' else 'ios' end as platform, date as install_date,
sum(impressions) as impressions, sum(taps) as clicks, sum(localSpend) as spend, sum(conversions) as installs
from adnetworks.apple_searchads 
group by 1,2,3,4,5,6,7,8,9,10,11
union all
select 'ironsource' as network, campaign_id, campaign_name, null as adset_id, null as adset_name, null as ad_id, null as ad_name, null as account_name,
lower(country_code) as country, case when lower(campaign_name) like '%android%' then 'android' else 'ios' end as platform, date as install_date,
sum(impressions) as impressions, sum(clicks) as clicks, sum(expense) as spend, sum(conversions) as installs
from adnetworks.supersonic_advertiser 
group by 1,2,3,4,5,6,7,8,9,10,11
union all
select 'google adwords' as network, CampaignID as campaign_id, Campaign as campaign_name, null as adset_id, null as adset_name, null as ad_id, null as ad_name, null as account_name,
lower(Location) as country, case when (lower(campaign) like '%android%' or lower(campaign) like '%and%') then 'android' else 'ios' end as platform, date as install_date, sum(Impressions) as impressions, sum(Clicks) as clicks, sum(cost) as spend, sum(conversions) as installs
from adnetworks.google_adwords_campaign_location --where date>=date_add(current_date,-200)
group by 1,2,3,4,5,6,7,8,9,10,11
union all
select 'unity ads' as network, campaign_id, campaign_name, null as adset_id, null as adset_name, null as ad_id, null as ad_name, null as account_name,
lower(country_code) as country, case when lower(campaign_name) like '%android%' then 'android' else 'ios' end as platform, date as install_date,
sum(0) as impressions, sum(clicks) as clicks, sum(spend) as spend, sum(installs) as installs
from adnetworks.unityads_advertiser 
group by 1,2,3,4,5,6,7,8,9,10,11

union all  
select case when source='Facebook' then 'facebook' 
            when source='Organic' then 'organic' 
            when source='AdWords' then 'google adwords' 
            when source='ironSource' then 'ironsource' 
            else 'others' end
            as network, null as campaign_id
            , app as campaign_name, null as adset_id, null as adset_name, null as ad_id, null as ad_name, null as account_name,
lower(country_field) as country, case when lower(platform) like '%android%' then 'android' when  lower(platform) like 'i%' then 'ios' end as platform, 
date as install_date,
sum(adn_impressions) as impressions, sum(custom_clicks) as clicks, sum(adn_cost) as spend, sum(custom_installs) as installs
from adnetworks.singular_advertiser where date<='2018-09-03'
group by 1,2,3,4,5,6,7,8,9,10,11

union all /* added 6/27 */
select 'liftoff' as network, campaign_group_id as campaign_id, campaign_id as campaign_name, null as adset_id, null as adset_name, null as ad_id, null as ad_name, null as account_name,
lower(country_id) as country, case when lower(campaign_id) like '%ios%' then 'ios' else 'android' end as platform, start_date as install_date,
sum(impressions) as impressions, sum(clicks) as clicks, sum(spend) as spend, sum(installs) as installs
from adnetworks.liftoff_advertiser 
group by 1,2,3,4,5,6,7,8,9,10,11

union all
select 'snapchat' as network, campaign_id, 
case when lower(campaign_name) like '%adquant_%' then substring(campaign_name,9,200)
     else campaign_name end as campaign_name,
null as adset_id, null as adset_name, ad_id, ad_name, null as account_name,
case when length(lower(split(campaign_name,'-')[2]))=2 then lower(split(campaign_name,'-')[2]) else 'null' end as country,
case when lower(campaign_name) like '%ios%' then 'ios' when lower(campaign_name) like '%android%' then 'android' end as platform,
date(start_time) as install_date, sum(impressions) as impressions, sum(swipes) as clicks, sum(spend) as spend, sum(0) as installs
from adnetworks.snapchat_advertiser 
group by 1,2,3,4,5,6,7,8,9,10,11

union all
select 'tapjoy' as network, null as campaign_id, name as campaign_name, null as adset_id, null as adset_name, null as ad_id, null as ad_name, null as account_name, lower(country) as country, lower(platform) as platform, date as install_date, sum(0) as impressions, 
sum(case when metrics='paid_clicks' then metricsvalue end) as clicks, 
sum(case when metrics='installs_spend' then -metricsvalue end) as spend, 
sum(0) as installs
from adnetworks.tapjoy_advertiser
group by 1,2,3,4,5,6,7,8,9,10,11

union all
select 'taptica' as network, campaignid as campaign_id, campaignName as campaign_name, null as adset_id, null as adset_name, null as ad_id, null as ad_name, null as account_name, lower(split(split(campaignname,'_')[0],' ')[3]) as country, case when lower(campaignName) like '%ios%' then 'ios' when lower(campaignName) like '%android%' then 'android' end as platform, date as install_date, sum(impressions) as impressions, sum(clicks) as clicks, sum(expense) as spend, sum(0) as installs
from adnetworks.taptica_advertiser
group by 1,2,3,4,5,6,7,8,9,10,11

union all
select 'crossinstall' as network, id as campaign_id, name as campaign_name, null as adset_id, null as adset_name, null as ad_id, null as ad_name, null as account_name,
lower(countries) as country, case when android='true' then 'android' else 'ios' end as platform, day as install_date, 
sum(impressions) as impressions, sum(clicks) as clicks, sum(cost) as spend, sum(conversions) as installs
from  adnetworks.crossinstall_advertiser
where length(countries)=2
group by 1,2,3,4,5,6,7,8,9,10,11

union all
select 'smadex' as network, campaign_id, campaign_name, null as adset_id, null as adset_name, creative_id as ad_id, creative_name as ad_name, account_name, lower(country) as country, case when lower(campaign_name) like '%and%' then 'android' else 'ios' end as platform, date(date) as install_date, sum(impressions) as impressions, sum(clicks) as clicks, sum(price) as spend, sum(0) as installs
from adnetworks.smadex_advertiser
group by 1,2,3,4,5,6,7,8,9,10,11
)

select 
network, campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name, 
country, platform, date(install_date) as install_date, impressions, clicks, spend, installs,
case when (lower(campaign_name) like '%pf-%' or lower(campaign_name) like '%pf\\_%' or lower(campaign_name) like '%pfh%' or lower(campaign_name) like '%photo%finish%') and lower(campaign_name) not like '%hrm%' then 'photo finish'
     when lower(campaign_name) like '%la-%' or lower(campaign_name) like '%la\\_%' or lower(campaign_name) like '%lng%' or lower(campaign_name) like '%languinis%' then 'languinis'      
     when lower(campaign_name) like '%stw%' or lower(campaign_name) like '%siege%' then 'siege' 
     when lower(campaign_name) like '%one%' or lower(campaign_name) like '%new%earth%' then 'operation new earth' 
     when lower(campaign_name) like '%tb-%' or lower(campaign_name) like '%tb\\_%' or lower(campaign_name) like '%tbs%' or lower(campaign_name) like '%tap%busters%' then 'tap busters' 
     when lower(campaign_name) like '%tgs%' or lower(campaign_name) like '%terra%' then 'terra genesis' 
     when (lower(campaign_name) like '%hrm%' or lower(campaign_name) like '%horse%racing%manager%') then 'horse racing manager' 
     when lower(campaign_name) like '%ftc%' or lower(campaign_name) like '%food%truck%' then 'food truck chef'
     when lower(campaign_name) like '%aah%' or lower(campaign_name) like '%almost%a%hero%' then 'almost a hero' 
     when lower(campaign_name) like '%dws%' or lower(campaign_name) like '%dwts%' or lower(campaign_name) like '%dancing%with%' then 'dancing with the stars' 
     when lower(campaign_name) like '%md-%' or lower(campaign_name) like '%md\\_%' or lower(campaign_name) like '%mtd%' or lower(campaign_name) like '%matador%' then 'matador'
     when lower(campaign_name) like '%nhm%' or lower(campaign_name) like '%nascar%heat%' then 'nascar heat'
     when lower(campaign_name) like '%nn-%' or lower(campaign_name) like '%nn\\_%' or lower(campaign_name) like '%nno%' or lower(campaign_name) like '%nitro%' then 'nitro nation'
     when lower(campaign_name) like '%tac%' or lower(campaign_name) like '%arcana%' then 'the arcana'
     when lower(campaign_name) like '%ddrp%' or lower(campaign_name) like '%ddr%' or lower(campaign_name) like '%diesel%' then 'diesel drag racing'
     when lower(campaign_name) like '%yee%' then 'yokee' -- added 6/14
     when lower(campaign_name) like '%lbn%' then 'linda brown' -- added 6/14
     when lower(campaign_name) like '%spw%' then 'spin to win'   -- added 6/14
     when lower(campaign_name) like '%bac%' then 'bake a cake'   -- added 6/14
     when lower(campaign_name) like '%sgd%' then 'seven guardians'   -- added 6/14
     when lower(campaign_name) like '%stt%' and lower(account_name) like '%snoopy%' then 'snoopy' -- added 6/14
     when (lower(campaign_name) like '%stt%' and (lower(account_name) like '%star%trek%' or lower(account_name) is null)) 
           or lower(campaign_name) like '%star%trek%'  then 'star trek timelines' 
     when lower(campaign_name) like '%rpg%' and lower(campaign_name) not like '%jrpg%' then 'dragon rpg'   -- added 6/12
     when lower(campaign_name) like '%smf%' then 'smashing four' -- added 6/12
     when lower(campaign_name) like '%bfg%' then 'battle for the galaxy' -- added 6/12
     when lower(campaign_name) like '%ofs%' or lower(campaign_name) like '%office%space%' then 'office space' -- added 7/17
     when lower(campaign_name) like '%pkp%' or lower(campaign_name) like '%pocket%politics%' then 'pocket politics' -- added 7/17
     when lower(campaign_name) like '%lkt%' or lower(campaign_name) like '%lucktastic%' then 'lucktastic' -- added 7/20 only fb
     when lower(campaign_name) like '%mng%' or lower(campaign_name) like '%mini%gun%' then 'mini gun' -- added 8/16 only google adwords
     when lower(campaign_name) like '%mtm%' or lower(campaign_name) like '%monster%merge%' then 'monster merge' -- added 8/16  only google adwords
     when lower(campaign_name) like '%tdt%' or lower(campaign_name) like '%two%dots%' then 'two dots' -- added 8/20
     when lower(campaign_name) like '%pcr%' or lower(campaign_name) like '%pacific%rim%' then 'pacific rim' -- added 8/27
     when lower(campaign_name) like '%thv%' or lower(campaign_name) like '%the%voice%' then 'the voice' -- added 11/09
     when lower(campaign_name) like '%whm%' or lower(campaign_name) like '%warhammer%' or lower(campaign_name) like '%sl0%' then 'warhammer' -- added 11/29
     when lower(campaign_name) like '%jes%' or lower(campaign_name) like '%jackpot%' then 'jackpot empire slots'
     when lower(campaign_name) like '%dcp%' then 'diesel challenge pro' -- added 02/21/2019
     when lower(campaign_name) like '%toy%' or lower(campaign_name) like '%toy%party%' then 'toy party' -- added 04/12/2019
     when lower(campaign_name) like '%mgg%' or lower(campaign_name) like '%mutant%' then 'mutants genetic gladiators' -- added 04/12/2019
     else 'others' end as game 
from spend_data

''','ua_spend',1)

# COMMAND ----------

ua_spend.coalesce(10).write.format('delta').mode('OverWrite').saveAsTable('{environment}.ds_summary_ua_spend'.format(environment=env))

# COMMAND ----------

