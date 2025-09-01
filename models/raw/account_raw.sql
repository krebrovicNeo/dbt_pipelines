{{ config(materialized='table') }}

with account as (select arr__c,
             cancellation_date__c,
             contract_start_date__c,
             createdbyid,
             createddate,
             date_became_customer__c,
             ownerid,
             a.intacctid__c,
             a.brand_name_formula__c,
             description,
             id,
             Full_Account_ID__c,
             Total_Employees__c,
             inbound_rep__c,
             industry,
             isdeleted,
             istam__c,
             last_call_date__c,
             lastmodifieddate,
             name,
             numberofemployees,
             other_industry__c,
             parentid,
             partner_type__c,
             hris_payroll__c,
             Attract__c,
             Benefits__c,
             CoreHR__c,
             eForms__c,
             GJobs__c,
             Insight__c,
             Onboard__c,
             Payroll__c,
             Perform__c,
             Engage__c,
             PowerLine__c,
             DMS__c,
             FTO__c,
             PowerStandards__c,
             PowerTime__c,
             Time_Attendance__c,
             PowerAction__c,
             PowerIA__c,
             PowerVitals__c,
             Learn__c,
             Vetted__c,
             Recall__c,
             Schedule__c,
             -- rating,
             reason_churned__c,
             reason_churned_description__c,
             recordtypeid,
             required_platform_start_date__c,
             salesloft_owner__c,
             shippingcity,
             shippingpostalcode,
             shippingstate,
             source__c,
             stage__c,
             brand_name__c,
             status__c,
             territory2__c,
             case
                 when id = '001Uh00000ENrRGIA1' then '00100000005xvIgAAI'
                 when id = '001Uh00000EOBnjIAH' then '00100000003aJPDAA2'
                 when id = '001Uh00000ENfk3IAD' then '0013700000L9iSxAAJ'
                 when id = '001Uh00000ENupVIAT' then '0018V00002aVuKVQA0'
                 when id = '001Uh00000EOBpaIAH' then '00100000004nDhHAAU'
                 when id = '001Uh00000EO7LaIAL' then '00100000005xz6AAAQ'
                 when id = '001Uh00000EO0LNIA1' then '00100000006umRqAAI'
                 when id = '001Uh00000ENyy1IAD' then '00100000004Z3AwAAK'
                 when id = '001Uh00000ENtNzIAL' then '0013700000Z6AyIAAV'
                 when id = '001Uh00000ENnW0IAL' then '00100000005yfmfAAA'
                 when id = '001Uh00000ENsfLIAT' then '0018V00002LApinQAD'
                 when id = '001Uh00000ENxrUIAT' then '0013700000MfaldAAB'
                 when id = '001Uh00000EO4EMIA1' then '00100000004nVt7AAE'
                 when id = '001Uh00000ENwtPIAT' then '0013700000MfavvAAB'
                 when id = '001Uh00000EOBSOIA5' then '00100000006KB6ZAAW'
                 when id = '001Uh00000ENzzwIAD' then '00100000006JEp2AAG'
                 when id = '001Uh00000Gl9MnIAJ' then '0013700000VNnfEAAT'
                 when id = '001Uh00000ENp1uIAD' then '00100000005xrNmAAI'
                 when id = '001Uh00000ENq26IAD' then '0013700000McR8NAAV'
                 when id = '001Uh00000EOBgTIAX' then '001000000039G5FAAU'
                 when id = '001Uh00000ENyq3IAD' then '0013700000MfbB5AAJ'
                 when id = '001Uh00000EO3PrIAL' then '00100000005DZijAAG'
                 when id = '001Uh00000EO3LcIAL' then '00100000005DQ2OAAW'
                 when id = '001Uh00000ENwT7IAL' then '00100000003aay9AAA'
                 when id = '001Uh00000EOBSkIAP' then '00100000006KBBAAA4'
                 when id = '001Uh00000EO88CIAT' then '0013700000JsfF0AAJ'
                 when id = '001Uh00000ENpppIAD' then '00100000008LxytAAC'
                 when id = '001Uh00000EO0NnIAL' then '00100000005U6vDAAS'
                 when id = '001Uh00000EOB6kIAH' then '00100000007oP3aAAE'
                 when id = '001Uh00000ENuJXIA1' then '00100000003nVpvAAE'
                 when id = '001Uh00000ENwbEIAT' then '00100000002gHbLAAU'
                 when id = '001Uh00000EO3MLIA1' then '00100000003akPAAAY'
                 when id = '001Uh00000EOByiIAH' then '00100000007oFNXAA2'
                 when id = '001Uh00000EO3NIIA1' then '00100000003cbShAAI'
                 when id = '001Uh00000ENyueIAD' then '00100000001V3Q1AAK'
                 when id = '001Uh00000EOASrIAP' then '0013700000LAIYVAA5'
                 when id = '001Uh00000ENywvIAD' then '001000000043aPQAAY'
                 when id = '001Uh00000EO8EsIAL' then '0013700000D0tAGAAZ'
                 when id = '001Uh00000EOA3PIAX' then '00100000005eSWiAAM'
                 when id = '001Uh00000ENwagIAD' then '00100000003ayzFAAQ'
                 when id = '001Uh00000ENo9JIAT' then '00100000008Ie9WAAS'
                 when id = '001Uh00000EO7exIAD' then '0013700000UovTOAAZ'
                 when id = '001Uh00000EO2JoIAL' then '00100000008KTUrAAO'
                 else
                     a.hcm_legacy_id__c end                                   hcm_legacy_id__c,
             referral_partner_content_provider__c,
             type,
             isduplicate__c,
             segmentation_tier__c,
             total_fte__c,
             accountnumber,
             Total_Value_of_Won_Opportunities__c,
             sysdate as                                                       mv_refresh_date,
             (partition_0 || '-' || partition_1 || '-' || partition_2)::date  partition_date,
             row_number() over (partition by id order by systemmodstamp desc) rn
      from {{ source('src','account') }} a
      where 1 = 1
        and (partition_0 || '-' || partition_1 || '-' || partition_2)::date =
            (select max((partition_0 || '-' || partition_1 || '-' || partition_2)::date) from {{ source('src','account') }})
        and isdeleted = 'false')
select *
from account
where rn = 1
