# nasa-dsc
NASA Distance Sample Counts

## Setup your environment

```shell
conda env create -f environment.yml
```


## Define the .env file

ER_SERVER=
ER_USERNAME=
ER_PASSWORD=
SINCE=2024-11-14 00:00:00+0000
UNTIL=2024-11-16 00:00:00+0000
ER_PATROL_TYPE=
ER_PATROL_SERIALS_FILTER=
ER_SUBJECT_FILTER=
TRAJ_COLUMNS = "['groupby_col','segment_start','segment_end','timespan_seconds','dist_meters', 'speed_kmhr', 'heading', 'geometry', 'junk_status','nsd', 'extra__id','extra__source','extra__subject__hex','extra__subject__id', 'extra__subject__name','extra__subject__subject_subtype', 'extra__subject__subject_type', 'extra__subject__user', 'extra__patrol_end_time', 'extra__patrol_id','extra__patrol_serial_number', 'extra__patrol_start_time','extra__patrol_title', 'extra__patrol_type','extra__patrol_type__display', 'extra__patrol_type__value',]"
