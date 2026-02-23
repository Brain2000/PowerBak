## SYNOPSIS

Copy/Sync Databases Between Two Servers Using Powershell.
Full, Differential, and Incremental (Log) are Supported.
Files Paths Can be Either an MSSQL Server or a File. The destination can stream to both simultaneously.


## DESCRIPTION

Copy/Sync Databases between two servers using Powershell.
Full, Differential, and Incremental (Log) are supported.
Files paths can be either an MSSQL server or a File. The destination can stream to both simultaneously.

Notes on incremental/differential backups:
A volume should be dedicated to SQL for MDF/LDF/UND files. This volume will be snapshot on given intervals.
A backup folder on a different volume should then be designated. It will contain MDF/LDF snapshots and incremental/differential backup files.
The following subfolder naming convention should be used:
   Incremental folder       : incrementals
   Differential folder      : differentials
   MDF/LDF snapshot exposure: snapshots

So for example if your backup folder is Z:\Backups\, it might contain the following folders:
   Z:\Backups\snapshots\20190114-1300\SQL\Data\MyDatabase.mdf / MyDatabase.ldf files
   Z:\Backups\incrementals\20190115-1300\MyDatabase.dat
   Z:\Backups\incrementals\20190116-1300\MyDatabase.dat

When restoring a database, you can give it the path and database name, such as Z:\Backups\MyDatabase, along with the date/time you want.
It will search through the snapshots for the closest date for the mdf/ldf files, and start with those.
It will then apply the proper differential/incrementals (depending on the -BackupType parameter) to bring it to the date/time you requested.


## Server Roles

The server this Powershell script is run from is the "controller". It connects to a "source" and "destination" server and sends them the necessary commands to facilitate the backup.
Status reports are sent back to the controller, which in turn displays real time progress on the screen.

This will allow the controller to direct two computers to back up to each other directly without having to pipe the backup through the controller itself.


## Source/Destination Streaming

The destination server creates a TCP listener. The source connects to the destination and pushes metadata to the destination, followed by the backup.
If there are multiple ToPaths, the destination will open all of them and then copy the incoming stream to each one simultaneously.
Status updates are sent between the source and destination via TCP OOB Messages.


## Communication Between Source and Destination

The source will connect to the destination, send a command, process the command, then disconect.
The source will connect again to the destination for the next command, and so on, and so on. This helps assure a clean start each time.
Each command wil be immediately followed up by a serialized MetaData class describing what the command needs to do (except "X").
Command List:
  "S" : A SQL stream will be sent for restore/saving (MetaData describes the SQL backup information).
  "T" : Same as "S", but wil disable the RESTART option during SQL restore, only used if the destination sends an OOB "T" message.
  "U" : Same as "S", but wil enable the CONTINUE_AFTER_ERROR option during SQL restore, only used if the destination sends an OOB "U" message.
  "F" : A raw file will be sent, not for SQL restore (MetaData describes the file).
  "B" : Perform a binary regex search/replace in a file (MetaData describes the file/search/replace).
  "E" : Execute a SQL command (MetaData describes the SQL command to run)
  "R" : Prepare a new database for autorestore (MetaData describes the new database information).
  "X" : All commands are done, close the listener and exit.

The source will send single character OOB Messages to indicate certain events.
Source OOB Indicators:
  "D" : This will be sent by the source to indicate that it has completed sending data.
        If the source was sending a file, the destination will send back a "D"/"A" if the file saved, and disconect.
        If the source was sending a SQL backup, the destination will send a "D" if no errors, "F" to switch to full backup, "T" to retry with NO RESTART option, or "A" if something else has gone wrong.
  "A" : This will be sent by the source to indicate that something has gone wrong and it is aborting.
        The destination will send a "A" back and move onto the next command.

The destination will send single character OOB Messages to indicate certain events.
Destination OOB Indicators:
  "D" : This will be sent by the destination to indicate that it has successfully processed all the data.
        If the source was sending a file, it will simply disconnect. If a SQL backup, it will notify SQL that the backup was successful, and disconnect.
  "X" : This is the same as "D", with the addition that we need to stop the backup/restore process, in which case the source will in turn shut down the listener with an "X" and exit.
  "A" : This will be sent by the destination to indicate that something has gone wrong and it is aborting.
        If the source was sending a backup directly from SQL backup, it will notify SQL that the backup failed so it does not mark a backup as having been completed.
  "P" : This will be sent by the destination to indicate that the item needs to be skipped. This is not an error condition, most likely it is due to a file already existing and -replace was not specified.
        If the source was sending a backup directly from SQL backup, it will notify SQL that the backup failed so it does not mark a backup as having been completed.
  "S" : This will be sent by the destination to indicate that the source should simply start over (used when the SQL volume goes offline)
        The source will notify SQL that the backup failed, disconnect, and reconnect with the "S" command
  "T" : This will be sent by the destination to indicate that the source should start over and use the "T" command.
        The source will notify SQL that the backup failed, disconnect, and reconnect with the "T" command, causing the destination to RESTORE without the SQL RESTART option.
  "U" : This will be sent by the destination to indicate that the source should start over and use the "U" command.
        The source will notify SQL that the backup failed, disconnect, and reconnect with the "U" command, causing the destination to RESTORE with the CONTINUE_AFTER_ERROR option.
  "F" : This will be sent by the destination to indicate that the source should start over and send a FULL backup.
        The source will notify SQL that the backup failed, disconnect, switch to a full backup mode, and start over with the "S" command.

Both the source and destinations will ALWAYS send eaach other exactly one OOB command, and wait for the other side to also send one. This signals that both sides agree to the outcome of a specific command.
Either side can initiate the first OOB message, depending on the circumstances, as an error condition can originate from either side preventing a backup from completing.


## Failure Notifications

When an event that causes a SQL or file copy to fail on either end, in an unrecoverable way, it will be collected so that it can be displayed/emailed in a report.


## Seeding Scenarios

When Differential or Incremental Backups are being used, databases must have an initial seeding done before these backups can be performed.
Seeding automatically happens. Here are some different scenarios that will trigger seeding:

 1. Existing databases that need to be seeded for the first time
 2. New customer database created or added
 3. Incremenal log chain becomes lost
 4. Destination database is deleted
 5. Destination database becomes corrupt
 6. Destination database restore does not complete, or cannot be resumed for some reason


.NOTES
PowerBak for MSSQL Server
Author: Brian Coverstone
Creation Date: 11/1/2018


## TODO

 1) Add -LimitNetworkBytesPerSecond parameter
 2) Add Encryption


## TESTS

 1. *Add parameter to randomize the order databases are run
 2. *Check for OOB messages even when a stream.read does not get any bytes?
 3. *Make it so a flush will wait until we confirm that the restore worked, otherwise it will abort. This will keep a database from losing a log that wasn't able to restore properly.
 4. *Add SEED option, so you can choose NOT to seed if a database needs it (will include option to skip filestream when seeding too), or to ONLY perform initial seeding for those that need it.
 5. *Fix remote session so multiple paths can be used
 6. *System databases must always perform a full backup. They should never restore to SQL, but a file destination is ok
 7. *Implement Replace command for restore command/file system (i.e. don't overwrite the file)
 8. *Find all errors on the destination restore that need to be recovered
 9. *Add MSDB history purge days for source/destination
 10. *Fix master database backup skip hanging source
 11. *Add ability to expand programming for destination filepaths
 12. *If a destination folder does not exist, create it
 13. *Add DeleteAfterNoBackupRestoreDays purge switch for destination
 14. *Aggregate backup into multiple jobs with configuration file
 15. *Test restore dbellomail from both databases to make sure the REPLACE option and email logging works properly
 16. *Volume snapshot management
 17. *Copy File src->dest (when restoring a database, to copy MDF/LDF/UND files to SQL server)
 18. *Write Auto Restore for snapshot mdf/ldf/und + logs, with point in time restore parameter
 19. *Add backupjob global options, such as SinceHours, so it will work with multiple jobs
 20. *Add ability to retry without RESTART option
 21. *Auto Restore should make sure the recovery type to NoRecovery for further log restores
 22. *Allow autorestore to pick a different destination database name
 23. *Allow autorestore to restore to a specific point in time (i.e. 2:00pm, 3:00pm)
 24. *Allow autorestore to bulk restore multiple databases in one operation
 25. *Clean up old log backups in incremental folders (and empty folders)
 26. *Add Src/Dest Credentials
 27. *Add numeric metrics for each type of backup plus errors (full, diff, incremental, failed)
 28. *Backup scheduling
 29. *Detect VSS error when drive goes offline and recover from the error
 30. *Add -CheckDB parameter
 31. *Write backups to .dat_ file, and rename it if it completes successfully, otherwise delete it.
 32. *Create VSS watchdog on destination servers that will create/delete a snapshot to increase the shadowstorage when necessary


## Parameters

.PARAMETER FromServer
The source server

.PARAMETER FromPath
The source path, either MSSQL:InstanceName\DBs, or a File Path such as C:\Backups\MyBackup.dat
Wildcards are accepted, such as MSSQL:Instance\a* or C:\Backups\a*.dat
MSSQL can also run a query using {squiggly brackets}, as long as it returns a "Name" field, such as "MSSQL:Instance\{SELECT Name FROM Sys.Databases WHERE Name NOT LIKE 'skip%'}"

.PARAMETER FromServerCredentials
The credentials to use when connecting to the source server.  The current account will be used if omitted.

.PARAMETER ToServer
The destination server

.PARAMETER ToPath
The destination path, either MSSQL:InstanceName, or a File Path such as C:\Backups\ (ending backslash optional)
To stream to two simultaneous paths, use a comma between them, such as "MSSQL:,C:\Backups\"
To expand an expression, use {squiggly brackets}, such as "C:\Backups\{[datetime]::Now.ToString('yyyyMMdd-HHmm')}\"

.PARAMETER ToServerCredentials
The credentials to use when connecting to the destination server. The current account will be used if omitted.

.PARAMETER BackupType
Full, Differential, Incremental. Differential and Incremental types will require an initial seeding.

.PARAMETER RecoveryType
The recovery mode a restored database is left in after being restored (applies only to differential and incremental backups).
Do not use the setting "Recovery" unless you absolutely mean to, as it will break further restores, requiring the database to be reseeded if it is to continue on.

.PARAMETER Reverse
When backing up and restoring between SQL instances, this will cause the backup database to go online (Recovery) and the primary database to apply the RecoveryType.
Only valid with with incremental backups.

.PARAMETER SkipWritingDBProperty
When backing up and restoring between SQL instances, normally a database property (POWERBAK_SOURCE) is set on the database to indicate where the source is located (in case the source isn't available later on).
Set this flag to prevent the Database Property from being written during a backup.

.PARAMETER SinceHours
Only process databases that have not been backed up since n hours ago.

.PARAMETER NetworkTimeout
Amount of time that the chain will wait for data. This This is in seconds.

.PARAMETER BufferSize
Buffer size in bytes used in network/file stream buffers. Applies to both the source and destination servers. (default = 1MB)

.PARAMETER TCPPort
TCP Port to use between Source/Destination, (0=Use Random port)

.PARAMETER CompressionLevel
Compression level to use (0=No compression, 6=Default)

.PARAMETER MaxTransferSize
MSSQL Backup/Restore MAXTRANSFERSIZE. This must be in 64Kb intervals (65536) and less than or equal to 4 MB. (default = 1MB)

.PARAMETER BufferCount
MSSQL Backup/Restore BUFFERCOUNT. This is multiplied by MaxTransferSize for the max memory to use for each BACKUP/RESTORE. Don't run yourself out of memory! (default = 100)

.PARAMETER SetIncrementalLoggingType
If you are performing an incremental backup, and a database is set to "Simple" recovery, it will automatically switch the logging recovery to either "Full" or "Bulk-Logged" in order for incremental backups to function.

.PARAMETER SkipFilePathOnInitialSeeding
When performing a differential or incremental backup, if the source or destination decide that a Full Seed is needed, this will cause the destination to skip any file paths, causing it to only send the initial seed to MSSQL:

.PARAMETER Replace
Set this switch to allow databases to be replaced if SQL does not recognize the database set being restored, and will also allow the file system to overwrite files with the same name in the folder specified.

.PARAMETER CopyOnly
When performing a backup, this will prevent the source database from "marking" the backup as occuring. Meaning, it will quietly create a "backup" that will not reset the DCM, BCM, or truncate the log.
An example of this would be to take a quarterly backup copy to a different destination server, using only the file system path, but you don't want to interrupt the backup/restore chain.

.PARAMETER ContinueAfterError
If a recoverable error occurs during restore, this will skip it and keep going... if possible...

.PARAMETER ToCheckDB
Check for database errors on the destination before restoring and incremental/differential.

.PARAMETER InitialSeeding
When performing a differential or incremental backup, this will allow initial seeding only, allow diff/incremental only, or allow a diff/incremental backup to turn into a full seed if necessary.
Useful if you want to pre-seed a backup, while running the diff/incremental backups along side, but you don't want the diff/incremental backup to suddenly start trying to seed missing databases that haven't been processed yet.

.PARAMETER PurgeBackupHistoryDays
When run as a maintenance job (or after performing a backup job), this will purge the MSDB history to events that have occured in the specified number of days, on both the source and destination servers (0 = skip).

.PARAMETER DeleteAfterNoBackupRestoreDays
When run as a maintenance job (or after performing a backup job), this will delete databases on the -ToServer -ToPath MSSQL: that have not had a backup or restore after the specified number of days (0 = skip).
Note: as a safety net, values 1 through 4 will automatically become 5.

.PARAMETER DeleteOldFiles
When run as a maintenance job (or after performing a backup job), this will delete old files on the -ToServer -ToPath for files that are older than the number of days specified (0 = skip).

.PARAMETER RandomizeOrder
When performing a backup, this will randomize the order so databases starting with "A" will not be the first to run (or maybe they will, it's random).

.PARAMETER SaveFilesAsBak
When the ToPath contains a path to a file, it will be saved as a BAK file instead of a compressed DAT file. This can also be used to copy a file from a DAT format to a BAK format.

.PARAMETER SQLDataPath
Override the SQL data path when restoring to a SQL server. Useful to split large backups between two destination paths attached to the same SQL instance.

.PARAMETER SQLLogPath
Override the SQL log path when restoring to a SQL server. Useful to split large backups between two destination paths attached to the same SQL instance.

.PARAMETER AppendFromServerFolderToPath
When performing a backup, this will append the -FromServer name as a folder at the end of a file path, to automatially keep backups separated.
If used in conjunction with -AppendDateTimeFolderToPath, the resulting path format will be similar to:
  Z:\Backups\Incremental\SQLServer1\20190101-1300\

This is particularly useful when performing multiple jobs simultaneously using -BackupJobs to keep each backup resource separate.

.PARAMETER AppendDateTimeFolderToPath
When performing a backup or snapshot, this will append a date/time folder in the format of "yyyyMMdd-HHmm" at the end of a file path, to automatially keep track of backups over time.
NOTE: The format "yyyyMMdd-HHmm" is configurable in the global configuration as setting "AutoFolderDateTime".
If used in conjunction with -AppendFromServerFolderToPath, the resulting path format will be similar to:
  Z:\Backups\Incremental\SQLServer1\20190101-1300\

.PARAMETER AutoRestoreToDateTime
This will restore a database from the source to the destination to the date/time requested.  Note that if you omit the time, it will use midnight. For the source path, only specify the root folder for backups plus database name.
For example: -FromPath C:\MyBackups\MyDatabase

.PARAMETER VSSWatchdog
This will watch the shadowstorage amount and create/delete a snapshot when the free space falls below the specified threshold. Use 0 to disable the VSSWatchdog.

.PARAMETER BackupJobs
This will set the parameters for a backup job from a configuration file instead of the command line arguments.
Configured Jobs are an array, which allows multiple jobs to be run simultaneously (see ParallelJobs).
NOTE: most command line parameters can also be specified with BackupJobs, but keep in mind that each parameter will add/override that parameter to ALL jobs in the set.
Make sure this is what you want because it can have unintended side effects if you don't know what you are doing.

Example BackupJobs format:

```
{
    Jobs:
    [
        {
            "FromServer":  "SQL1",
            "FromPath":  "MSSQL:INSTANCENAME\\*",

            "ToServer":  "localhost",
            "ToPath":  [ "MSSQL:BAKINSTANCENAME", "Z:\\MyBackups\\Incrementals\\" ],

            "AppendFromServerFolderToPath":  { IsPresent":  true },
            "AppendDateTimeFolderToPath":  { "IsPresent":  true }

            "BackupType":  "Incremental",
            "RecoveryType":  "Standby",
            "SkipFilePathOnInitialSeeding":  { "IsPresent":  true },
            "Replace":  { "IsPresent":  true },

            "PurgeBackupHistoryDays":  200,
            "DeleteAfterNoBackupRestoreDays":  180,
        },
        {
            ...repeat for each job...
            ...maintenance job can be at the end...
        },
        {
            "NoOverrides":  true, <-- special parameter to prevent additional command line arguments from being added to this maintenance config entry
            "ToServer":  "localhost",
            "ToPath":  [ "Z:\\MyBackups\\Incrementals\\" ],
            "DeleteOldFiles":  45,

            "SnapshotManagement":  {
                "Volume":  "E:",
                "Expose":  "Z:\\MyBackups\\Snapshots\\",
                "IntervalDays":  15,
                "DeleteAfterDays":  45
            },
            "AppendDateTimeFolderToPath":  { "IsPresent":  true }
        }
    ]
}
```

.PARAMETER ParallelJobs
Used in combination with BackupJobs when multiple jobs are in the configured, this specifies how many will run at one time. When one job completes, the next one will be queued.
NOTE: A value of 0 means unlimited, which is the default.

.PARAMETER EmailNotification
Will send an email after a command or job has completed with a summary, metrics, and errors that occurred.

.PARAMETER SnapshotManagement
This will snapshot a volume after a backup has completed.
Arguments are in the form of a dictionary with the following entries:
    IntervalDays   : Snapshot every so many days
              - or -
    DaysOfMonth    : The days of the month to perform a snapshot
    Volume         : The volume to snapshot
    Expose         : (optional) The location to expose the snapshot, should be a base folder, snapshots can automatically append a folder with the date/time with -AppendDateTimeFolderToPath, i.e. Expose="C:\Backups\Snapshots" -> C:\Backups\Snapshots\20190201-1300\
    DeleteAfterDays: Number of days to keep snapshots before deleting them

.PARAMETER ScheduledTask
This is a dictionary of parameters to create a scheduled task.
   TaskName: <name>
  StartTime: <time of day>
DayInterval: <default = 1>
    LogonId: <user or group>
  LogonType: <default = Password>, use enum [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.LogonTypeEnum]
   RunLevel: <default = Limited>, use enum [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.RunLevelEnum]

.PARAMETER NoOverrides
This is a special parameter that you can apply to a job configuration in a file. It will prevent any of the command line arguments from overriding or adding to the configuration for that particular job.
The use case for this would be a volume snapshot maintenance job.


## Examples

PowerBak -FromServer SQLServer1 -FromPath MSSQL:\* -ToServer localhost -ToPath MSSQL: -BackupType Full -CopyOnly

PowerBak -FromServer SQLServer2 -FromPath "MSSQL:SrcInstance\{SELECT Name FROM Sys.Databases WHERE Name NOT IN ('skip1', 'skip2')}" -ToServer localhost -ToPath MSSQL:DestInstance,"C:\MyBackups\Incrementals\" -AppendFromServerFolderToPath -AppendDateTimeFolderToPath

PowerBak -FromServer SQLServer1 -FromPath MSSQL:\* -ToServer localhost -ToPath MSSQL:,"C:\MyBackups\Incrementals\" -AppendFromServerFolderToPath -AppendDateTimeFolderToPath -SinceHours 24

PowerBak -FromServer SQLServer1 -FromPath MSSQL:\* -ToServer localhost -ToPath MSSQL:,"C:\MyBackups\Incrementals\SQLServer1\{[DateTime]::Now.ToString('yyyyMMdd-HHmm')}\" -SinceHours 24

PowerBak -FromServer localhost -FromPath C:\MyBackups\MyDatabase -ToServer SQLServer3 -ToPath MSSQL:NewInstance -AutoRestoreToDateTime "2019-01-14 1:00:00 PM"

PowerBak -FromServer SQLServer2 -FromPath MSSQL:\* -ToServer localhost -ToPath MSSQL:,C:\MyBackups\Incrementals\ -SnapshotManagement @{Volume="Z:"; Expose="C:\MyBackups\Snapshots\"; DaysOfMonth=1,15; DeleteAfterDays=45;} -AppendFromServerFolderToPath -AppendDateTimeFolderToPath

PowerBak -FromServer localhost -FromPath C:\MyBackups\MyDatabase.dat -ToServer localhost -ToPath C:\MyBackups\ -SaveFilesAsBak

PowerBak -BackupJobs C:\PowerBak\AllJobs.config -EmailNotification

PowerBak -BackupJobs C:\PowerBak\AllJobs.config -SinceHours 24

PowerBak -BackupJobs C:\PowerBak\AllJobs.config -ScheduledTask @{TaskName="MyBackup"; StartTime="8:00 PM"; LogonId="NETWORKSERVICE"; LogonType="ServiceAccount"; RunLevel="Highest"; EmailNotification}
