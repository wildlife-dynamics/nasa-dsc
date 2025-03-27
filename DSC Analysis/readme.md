- The DSC_Analysis.py file requires an .env file with the following structure:

        EXPORT_TIME_ZONE="Africa/Nairobi"
        EE_PROJECT=<your EE project name>
        EVENT_COLUMN_TRANSFORM='{
            "time": "time",
            "event_type": "event_type", 
            "title": "title", 
            "geometry": "geometry",
            "serial_number": "serial_number", 
            "patrol_id": "patrol_id", 
            "patrol_serial_number": "patrol_serial_number",
            "event_details__distancecountpatrol_transectid": "transect_id",
            "event_details__distancecountpatrol_numberofobservers": "num_observers",
            "event_details__distancecountwildlife_species": "species",
            "event_details__distancecountwildlife_totalcount": "totalcount",
            "event_details__distancecountwildlife_numberofjuveniles": "num_juveniles",
            "event_details__distancecountwildlife_radialangle": "radialangle",
            "event_details__distancecountwildlife_distancetocentre": "dist_to_centre"
        }'

        ER_USERNAME=<user>
        ER_PASSWORD=<password>
        ER_PATROL_TYPE=<Patrol Type UUID>
        SURVEY_NAME=Site_Survey_YYYY_MM
        SINCE=YYYY-MM-DD HH:MM:SS+0000
        UNTIL=YYYY-MM-DD HH:MM:SS+0000
        ER_SPATIAL_TRANSECTS_GROUPID=<Spatial Feature Group UUID>


- The Script also erquires a valid EE authentication token is available on the execution machine. 