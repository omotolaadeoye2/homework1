# Homework1
Homework 1 for EcZachly Bootcamp3

Question 1

Create Table oadeoy2.actors 
(
  actor varchar,
  actorid integer,
  films array(row(
    film varchar,
    votes integer,
    filmid int))
  quality_class varchar,
  is_active boolean,
  current_year integer
)
With 
(
format = "Parquet",
patritioning = array ['current_year']
)

Question 2

Insert into oadeoy2.actors
With
  last_year as (
    Select *
    from oadeoy2.actors
    where current_year = 1995
    ),
  this_year as (
  Select *
  from oadeoy2.actors
  where year = 1996
    )
Select 
  Coalesce (ly.actor, ty.actor) as actor,
  Coalesce (ly.actorid, ty.actorid) as actorid,
  Case when ly.current_year is null then
ty.current_year 
  when ty.current_year is not null and 
  ls.current_year is null then 
  array[row(
  ty.film,
  ty.votes,
  ty.rating,
  ty.filmid)
  ]
  when ty.current_year is not null and
  ly.current_year is not null then
  array[row(
  ty.film,
  ty.votes,
  ty.rating,
  ty.filmid)
  ] || ly.current_year) end as current_year,
  Avg(Case 
  when ty.rating > 8 then 'star'
  when ty.rating > 7 then 'good'
  when ty.rating > 6 then 'average'
  else 'bad')
  end as quality_class,
  Case 
    when ty.current_year is not null  then true
    else false
    end as is_active 
  Coalesce (ty.current_year, ls.current_year + 1) as current_year
  From last_year ly
  full outer join this_year ty on
ly.actor = ty.actor

Question 3
Create Table actors_history_scd 
(
  actor varchar
  quality_class varchar,
  is_active boolean,
  start_date integer,
  end_date integer,
  current_year integer
)
With
(
  format = 'Parquet',
  partition = Array['current_year']
)


Question 4

Insert into oadeoy2.actors_history_scd
With 
  lagged as 
  (
    Select 
      actor,
      Case 
        When is_active then 1 
        Then 0 
      End as is_active
      Case 
        When Lag(is_active, 1) over (
          Partition by actor
          order by 
            current_date) Then 1
            Else 0
        End as is_active_to_year,
        current_date
        From oadeoy2.actors
         where current_year < = 2001
         
   ),
   streaked as
   (
     Select *,
       Sum(Case 
         when is_active<>
         is_active_to_year then 1
         else 0
         end
         ) Over
         (Partition by 
           actor
           order by 
           current_year
           ) as streak_identifier
       From lagged
       )
 Select
   actor,
   max(is_active) = 1 as is_active,
   min(current_year) as start_date,
   max(current_year) as end_date
   2001 as current_year
 from 
   streaked
 group by 
   actor,
   streak_identifier 


Question 5

Insert into oadeoy2.actors_history_scd
With 
  last_year_scd as 
  (
  Select *
  from oadeoy2.actors_history_scd
  where
    current_year = 2001
    ),
  current_year_scd as
  ( 
  Select *
  from oadeoy2.actors_history_scd
  where current_year = 2002
  ),
  combined as
  (
  Select 
  Coalesce (ly.actor, cy.actor) as actor,
  Coalesce (ly.start_date, cy.start_date) as start_date,
  Coalesce (ly.end_date, cy.end_date) as end_date,
  Case
    when ly.is_active <> cy.is_active
  then 1
    when ly.is_active = cy.is_active
  then 0
    end as did_change,
    ly.is_active as
  is_active_last_year,
    cs.is_active as
  is_active_current_year,
  2002 as current_year
from 
  last_year_scd ly
  full outer join current_year_scd cy
  on
  ly.actor = cy.actor and ly.end_date +1 = current_year
  ),
  changes as
  (
    Select 
      actor,
      current_year,
      CASE
        WHEN did_change = 0 THEN ARRAY[
          CAST(
            ROW (
              is_active_last_year,
              start_date,
              end_date + 1
            ) AS ROW (
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        WHEN did_change = 1 THEN ARRAY[
          CAST(
            ROW (is_active_last_year, start_date, end_date) AS ROW (
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          ),
          CAST(
            ROW (
              is_active_current_year,
              current_year,
              current_year
            ) AS ROW (
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        WHEN did_change IS NULL THEN ARRAY[
          CAST(
            ROW (
              COALESCE(is_active_last_year, is_active_current_year),
              start_season,
              end_season
            ) AS ROW (
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
      END AS change_array
    FROM
      combined
  )
SELECT
  actor,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_date
FROM
changes
  CROSS JOIN UNNEST (change_array) AS arr    
        
         
