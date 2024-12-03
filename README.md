# nasa-dsc
NASA Distance Sample Counts

## Setup your environment

```shell
conda env create -f environment.yml
```


## Define the patrols_to_gpkg .env file

ER_SERVER=
ER_USERNAME=
ER_PASSWORD=
SINCE=2024-11-14 00:00:00+0000
UNTIL=2024-11-15 00:00:00+0000
ER_PATROL_TYPE=2bbe760a-21b2-48f1-9457-45d372295dfe
SURVEY_NUMBER=SURVEY_1
ER_PATROL_SERIALS_FILTER=None
ER_SUBJECT_FILTER=None
RELOCS_COLUMNS = "['extra__id', 'extra__source','geometry', 'extra__subject__name','extra__subject__subject_type', 'extra__subject__subject_subtype', 'extra__subject__user','extra__subject__hex', 'groupby_col', 'fixtime', 'patrol_id', 'patrol_title', 'patrol_serial_number','patrol_start_time', 'patrol_end_time', 'patrol_type','patrol_type__value', 'patrol_type__display']"
TRAJ_COLUMNS = "['groupby_col','segment_start','segment_end','timespan_seconds','dist_meters', 'speed_kmhr', 'heading', 'geometry', 'junk_status','nsd', 'extra__id','extra__source','extra__subject__hex','extra__subject__id', 'extra__subject__name','extra__subject__subject_subtype', 'extra__subject__subject_type', 'extra__subject__user', 'extra__patrol_end_time', 'extra__patrol_id','extra__patrol_serial_number', 'extra__patrol_start_time','extra__patrol_title', 'extra__patrol_type','extra__patrol_type__display', 'extra__patrol_type__value',]"
EVENT_COLUMNS="['id', 'time', 'event_type', 'comment', 'title', 'reported_by','patrol_segments', 'geometry', 'serial_number', 'event_category', 'patrol_id','patrol_serial_number','event_details__distancecountwildlife_species','event_details__distancecountwildlife_totalcount', 'event_details__distancecountwildlife_radialangle','event_details__distancecountwildlife_distancetocentre', 'event_details__distancecountwildlife_numberofjuveniles','event_details__updates','event_details__distancecountpatrol_transectid','event_details__distancecountpatrol_teammembers','event_details__distancecountpatrol_numberofobservers']"
