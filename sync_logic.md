


# Commands for troubleshooting deadlocks
`show processlist`
`SHOW ENGINE INNODB STATUS`
```
SELECT OBJECT_TYPE,
       OBJECT_SCHEMA,
       OBJECT_NAME,
       LOCK_TYPE,
       LOCK_STATUS,
       THREAD_ID,
       PROCESSLIST_ID,
       PROCESSLIST_INFO
FROM performance_schema.metadata_locks
INNER JOIN performance_schema.threads ON THREAD_ID = OWNER_THREAD_ID
WHERE PROCESSLIST_ID <> CONNECTION_ID();
```



to Add:
races:
    race_status
    race_distance

horse:
    insert missing horses
    add name, sire

jokey:
    insert missing jokeys
    fist name, last name

trainer:
    inser missing trainers
    first  name, last name

race_results:
    jokey_id
    trainer_id
    scratched
    morning_line **
    wps_win
    wps_show
    wps_place
    

wps_wager_type (exotic bet)
    inster missing