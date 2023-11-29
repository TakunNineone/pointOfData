with 
df as 
(
        select replace(entity,'.xsd','-definition.xml') entity,array_agg(distinct dim_def) dim_def
        from
        (
        select tp.entity,unnest(dim_def) dim_def
        from tableparts tp 
        join tables t on t.version=tp.version and t.namespace=tp.uri_table and t.rinok=tp.rinok
        left join (
                select dict_entity,array_agg(dim||'#'||mem) dim_def
                from
                (
                select e.qname dim,em.qname mem,e.entity,em.entity,a.entity,split_part(a.entity,'-definition.xml',1)||'.xsd' dict_entity,a.arcrole
                from locators l 
                join arcs a on a.version=l.version and a.rinok=l.rinok and a.entity=l.entity and a.arcfrom=l.label and a.parentrole=l.parentrole and arcrole='http://xbrl.org/int/dim/arcrole/dimension-default'
                join locators lm on  a.version=lm.version and a.rinok=lm.rinok and a.entity=lm.entity and a.arcto=lm.label and a.parentrole=lm.parentrole
                join elements e on e.id=l.href_id and e.version=l.version
                join elements em on em.id=lm.href_id and em.version=lm.version
                ) z
                group by dict_entity
                ) df on df.dict_entity = ANY(string_to_array(imports,';'))
        ) ee 
        group by replace(entity,'.xsd','-definition.xml')
),
def_temp as not materialized
(
 select l.version,l.rinok,l.entity,l.parentrole,e.qname,l.label,arcfrom,arcto,arcrole,e.type,coalesce(e.abstract,'false') abstract,a.usable,targetrole,
         case when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' and coalesce(e.type,'')!='nonnum:domainItemType' then 1
         when arcrole='http://xbrl.org/int/dim/arcrole/hypercube-dimension' then 2
         when arcrole='http://xbrl.org/int/dim/arcrole/dimension-domain' then 3
         when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' then 4 
         when arcrole='http://xbrl.org/int/dim/arcrole/notAll' then 5 
         when arcrole='http://xbrl.org/int/dim/arcrole/all' then 0 else -1 end type_elem,
  typeddomainref
        from locators l
        join elements e on e.id=href_id and e.version=l.version 
        join arcs a on a.arcto=l.label and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole
        and a.arctype='definition' and l.rinok!='bfo'
		where lower(l.parentrole) not similar to '%chasti%'
	--and l.parentrole in ('http://www.cbr.ru/xbrl/nso/ins/rep/2024-11-01/tab/sr_0420155/P_20_1_UG_1','http://www.cbr.ru/xbrl/nso/ins/rep/2024-11-01/tab/sr_0420158_R4/1')
	--and l.entity in ('sr_0420760_kbki-definition.xml','sr_0420760_bki_zayavl-definition.xml')
        order by arcrole
),
def as
(
select l.version,d_.rinok,d_.entity,d_.parentrole,e.qname,l.label,arcfrom,arcto,arcrole,e.type,coalesce(e.abstract,'false') abstract,a.usable,null targetrole,
         case when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' and coalesce(e.type,'')!='nonnum:domainItemType' then 1
         when arcrole='http://xbrl.org/int/dim/arcrole/hypercube-dimension' then 2
         when arcrole='http://xbrl.org/int/dim/arcrole/dimension-domain' then 3
         when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' then 4 
         when arcrole='http://xbrl.org/int/dim/arcrole/notAll' then 5 
         when arcrole='http://xbrl.org/int/dim/arcrole/all' then 0 else -1 end type_elem,
  typeddomainref
        from locators l
		join (select distinct version,rinok,entity,parentrole,targetrole from def_temp 
where parentrole not in ('http://www.cbr.ru/xbrl/nso/ins/rep/2024-11-01/tab/sr_0420154/sr_0420154_R2_P23_2_1',
'http://www.cbr.ru/xbrl/nso/operatory/rep/2024-11-01/tab/sr_0420723_r3_1',
'http://www.cbr.ru/xbrl/nso/operatory/rep/2024-11-01/tab/sr_0420723_r3_2',
'http://www.cbr.ru/xbrl/nso/operatory/rep/2024-11-01/tab/sr_0420934_r4',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420415/SR_0420415_R4_PR4_1',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420415/SR_0420415_R4_PR4_1_dop',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420415_R1_PR_1_2/SR_0420415_R1_PR_1_2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420415_R1_PR_1_2/SR_0420415_R1_PR_1_2_2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_4',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_4_2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_5',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_5_2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_6',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_6_2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_7',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_7_2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_7_3',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_7_4',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_7_5',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_7_6',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_8',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_8_2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_9',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R_9_2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R1_P1_2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R1_P1_2_2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R1_P1_2_3',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R1_P1_2_4',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R1_P1_3',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R2_2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R3',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R3_2',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R3_3',
'http://www.cbr.ru/xbrl/nso/purcb/rep/2024-11-01/tab/SR_0420431_R1_9_R3_4')
) d_ on d_.version=l.version and d_.targetrole=l.parentrole
        join elements e on e.id=href_id and e.version=l.version 
        join arcs a on a.arcto=l.label and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole
        and a.arctype='definition'
	
	union all
	
	select * from def_temp
	
	
),
dd as
(
WITH RECURSIVE recursive_hierarchy AS ( 
  SELECT 
    version,rinok,entity,parentrole,targetrole,usable,qname AS child_qname, 
    qname AS parent_qname,  -- Сохраняем "qname" родителя 
    arcfrom, 
    arcto, 
    label, 
    type_elem,typeddomainref
  FROM 
    def z
 
  WHERE 
    type_elem = 2  -- Начинаем с элементов типа 2 
 
  UNION ALL 
 
  SELECT 
    c.version,c.rinok,c.entity,c.parentrole,c.targetrole,c.usable,c.qname AS child_qname, 
    p.parent_qname,  -- Передаем "qname" родителя 
    c.arcfrom, 
    c.arcto, 
    c.label, 
    c.type_elem,c.typeddomainref
  FROM 
    def c 
  INNER JOIN 
    recursive_hierarchy p ON c.arcfrom = p.arcto and c.version=p.version and c.rinok=p.rinok and c.entity=p.entity and c.parentrole=p.parentrole  
  WHERE 
    c.type_elem IN (3, 4)  -- Дети могут быть типа 3 или 4 
)

select version,rinok,entity,parentrole,array_agg(dims) dims
 from
 (
select version,rinok,entity,parentrole,targetrole,usable,parent_qname dim,
string_agg(distinct parent_qname||case when parent_qname in ('dim-int:Kod_ValyutyAxis') or parent_qname=child_qname
		   then '' else '#' end||case when parent_qname in ('dim-int:Kod_ValyutyAxis') or parent_qname=child_qname
		   then '' else child_qname end,'|') dims
FROM 
recursive_hierarchy
where ((type_elem>=2 and typeddomainref is null and parent_qname!=child_qname) or (type_elem>=2 and typeddomainref is not null) or type_elem>2)
group by version,rinok,entity,parentrole,targetrole,parent_qname,usable
  ) dd
where coalesce(usable,'true')!='false'
group by version,rinok,entity,parentrole	
	)

select distinct version,zz.rinok,zz.entity,concept,parentrole,array_unique(dimensions) dims,coalesce(eps,'') eps
  from
  (
    select distinct dd.version,dd.rinok,dd.entity,parentrole,rt.definition parentrole_text,concept,remove_elements_from_array(dims,dim_def) dimensions,dims
        from
        (
        select cc.version,cc.rinok,cc.entity,cc.parentrole,cc.qname concept,dims,
  compare_arrays(dims,dims_minus) is_minus
        from 
        (
        select version,rinok,entity,parentrole,qname,arcfrom,label,usable,targetrole 
        from def
        where type_elem=1
        and abstract='false'
        ) cc 
        left join dd using (version,rinok,entity,parentrole)
        left join 
        (
  select d1.version,d1.rinok,d1.entity,d1.parentrole,d1.arcfrom,array_agg(array_to_string(dims,';')) dims_minus
        from def d1 
        join dd d2 on d1.version=d2.version and d1.rinok=d2.rinok and d2.parentrole=d1.targetrole
  and d1.type_elem=5
  group by d1.version,d1.rinok,d1.entity,d1.parentrole,d1.arcfrom
        ) tr on tr.version=cc.version and cc.rinok=tr.rinok and cc.entity=tr.entity and cc.parentrole=tr.parentrole and tr.arcfrom=cc.label
  ) dd
  left join df on df.entity=dd.entity 
        left join roletypes rt on rt.roleuri=dd.parentrole
        where is_minus=0
  ) zz
  left join 
  (
  select distinct rinok,replace(split_part(schemalocation,'/',-1),'.xsd','-definition.xml') entity,string_agg(distinct split_part(targetnamespace,'/',-1),';') eps
from tables t
	  where lower(split_part(targetnamespace,'/',-1)) not similar to '%support%'
group by rinok,replace(split_part(schemalocation,'/',-1),'.xsd','-definition.xml')
order by 1
  ) ee on ee.rinok=zz.rinok and ee.entity=zz.entity
order by version,rinok,entity,parentrole,concept

