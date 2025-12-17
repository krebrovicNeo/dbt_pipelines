{{ config(materialized='table') }}

select *
from (select id,
             accountid,
             calldisposition,
             calldurationinseconds,
             original_lead_gen_role__c,
             createddate,
             OwnerId,
             CompletedDateTime,
             ActivityDate,
             LastModifiedById,
             Status,
             sequence_step_number__c,
             Original_booked_For_role__c,
             WhoId,
             a.hcm_legacy_id__c,
             no_of_calls__c,
             a.Sequence_Name__c,
             a.priority,
             activity_type__c,
             crms22__Summary_Comments__c,
             lead_rating__c,
             a.correct_contact__c,
             a.call_start_time__c,
             subject,
             a.calltype,
             isdeleted,
             createdbyid,
             a.Contact_Title__c,
             a.Disposition__c,
             Outreach_Attributed_Sequence_Name__c,
             Sequence_ID__c,
             sysdate                                        as                mv_refresh_date,
             partition_0 || partition_1 || partition_2::int as                partition_date,
             row_number() over (partition by id order by systemmodstamp desc) rn
      from dl_bi.task a
      where 1 = 1
        and (partition_0 || '-' || partition_1 || '-' || partition_2)::date =
            (select max((partition_0 || '-' || partition_1 || '-' || partition_2)::date) from dl_bi.task)
        and a.isdeleted = 'false')
where rn = 1