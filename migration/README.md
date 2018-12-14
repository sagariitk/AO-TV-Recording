# Migration for Local database


This script uploads local database to cloud SQL.
Only data of primary device is sent to cloud SQL for further processing.
As per now the columns are hardcoded, and the Table schema on both databases should be same.
This script can work of any database migration, just specify DB credentials.

## Generic Methods

#### db_operation
the task of this method is to execute a SQL query.
It takes a sql query, db connection and cursor as input.

#### get_timestamp
This function converts a SQL timestamp(datetime object in python) to a string datatype.
This is done so that it can be used in SQL queries.

#### get_row_column
This function takes tuple number & tuple(data) as input arguments and returns values (not NULL) and their corresponding cloumns.
This function acts as a formatter, it removes extra strings appended in the data retrieved from SQL select query.

#### send_email
This function takes the rows which is to be mailed and checks if any data is present.
If data is presend it calls generate_email_method for mormatting the email body to be sent.
And then it sends the POST request to the cloud endpoint which sends email to the users mentioned.
This function is only for <b> recording_tracking </b> table.

#### generate_email_message
This method is for formatting the message body of email.
Only the desired columns which are to be displayed are selected and a HTML table is formed.


## Daily Migration Script
Migrates data on daily basis. 
### Methods

#### get_date
the task of this function is to get the current and tomorrow's date.

#### main
queries data from recording_tracking table, recording, invalid_frame_tracking and recording limit.
If there is any data to be migrated then, connection to Cloud SQL is made and one by one each tuple is inserted to instance.
After each insertion migration_status on local is updated.

## Weekly Migration Script
Migrates data generated between monday to friday , on saturday.
### Methods

#### get_date
the task of this function is to get the date of occured monday and friday.
if you run this script on saturday or sunday then the dates will be of current week
and if you run in on any day before friday then the date of previous week will be returned.

#### main
queries twice data from local table, one for migration and other for sending to email.
If there is any data to be migrated then, connection to Cloud SQL is made and one by one each tuple is inserted to instance.
After each insertion migration_status on local is updated.