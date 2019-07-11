# Welcome to EasyArchiver!

This project contains a Powershell script that works as an archiving/backup job and moves data from Couchbase to SQL Server. It can also optionaly remove the data from the Couchbase db after it has been moved.

For those who want to use this script as is need only to change the parameters listed at the bottom of it. The others can feel free to play with the code if they want to use different kind of db technologies or do some other additional works.
For those who want to use this script as is need only to change the parameters listed at the bottom of it. The others can feel free to play with the code if they want to use different kind of db technologies or do some other additional works.

Script simply consists of 2 functions that move data from Couchbase to SQL Server (Get-CouchbaseData) and removes the source data (Remove-CouchbaseData). You can optionaly work with only "Get-CouchbaseData" or both depending on your needs.

This script can be configured as a job that works periodically (for ex; once in every 2 minutes) and the select query ($selectQuery) can be limited with some specific number that helps you to keep data size at some certain level. 