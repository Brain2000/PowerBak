<#
.SYNOPSIS
Copy/Sync Databases Between Two Servers Using Powershell.
Full, Differential, and Incremental (Log) are Supported.
Files Paths Can be Either an MSSQL Server or a File. The destination can stream to both simultaneously.


.DESCRIPTION
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


Server Roles
------------
The server this Powershell script is run from is the "controller". It connects to a "source" and "destination" server and sends them the necessary commands to facilitate the backup.
Status reports are sent back to the controller, which in turn displays real time progress on the screen.

This will allow the controller to direct two computers to back up to each other directly without having to pipe the backup through the controller itself.


Source/Destination Streaming
----------------------------
The destination server creates a TCP listener. The source connects to the destination and pushes metadata to the destination, followed by the backup.
If there are multiple ToPaths, the destination will open all of them and then copy the incoming stream to each one simultaneously.
Status updates are sent between the source and destination via TCP OOB Messages.


Communication Between Source and Destination
--------------------------------------------
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


Failure Notifications
---------------------
When an event that causes a SQL or file copy to fail on either end, in an unrecoverable way, it will be collected so that it can be displayed/emailed in a report.


Seeding Scenarios
-----------------
When Differential or Incremental Backups are being used, databases must have an initial seeding done before these backups can be performed.
Seeding automatically happens. Here are some different scenarios that will trigger seeding:

 1) Existing databases that need to be seeded for the first time
 2) New customer database created or added
 3) Incremenal log chain becomes lost
 4) Destination database is deleted
 5) Destination database becomes corrupt
 6) Destination database restore does not complete, or cannot be resumed for some reason


.NOTES
PowerBak for MSSQL Server
Author: Brian Coverstone
Creation Date: 11/1/2018


TODO
----
 1) Add -LimitNetworkBytesPerSecond parameter
 2) Add Encryption


TESTS
-----
*1) Add parameter to randomize the order databases are run
*2) Check for OOB messages even when a stream.read does not get any bytes?
*3) Make it so a flush will wait until we confirm that the restore worked, otherwise it will abort. This will keep a database from losing a log that wasn't able to restore properly.
*4) Add SEED option, so you can choose NOT to seed if a database needs it (will include option to skip filestream when seeding too), or to ONLY perform initial seeding for those that need it.
*5) Fix remote session so multiple paths can be used
*6) System databases must always perform a full backup. They should never restore to SQL, but a file destination is ok
*7) Implement Replace command for restore command/file system (i.e. don't overwrite the file)
*8) Find all errors on the destination restore that need to be recovered
*9) Add MSDB history purge days for source/destination
*10) Fix master database backup skip hanging source
*11) Add ability to expand programming for destination filepaths
*12) If a destination folder does not exist, create it
*13) Add DeleteAfterNoBackupRestoreDays purge switch for destination
*14) Aggregate backup into multiple jobs with configuration file
*15) Test restore dbellomail from both databases to make sure the REPLACE option and email logging works properly
*16) Volume snapshot management
*17) Copy File src->dest (when restoring a database, to copy MDF/LDF/UND files to SQL server)
*18) Write Auto Restore for snapshot mdf/ldf/und + logs, with point in time restore parameter
*19) Add backupjob global options, such as SinceHours, so it will work with multiple jobs
*20) Add ability to retry without RESTART option
*21) Auto Restore should make sure the recovery type to NoRecovery for further log restores
*22) Allow autorestore to pick a different destination database name
*23) Allow autorestore to restore to a specific point in time (i.e. 2:00pm, 3:00pm)
*24) Allow autorestore to bulk restore multiple databases in one operation
*25) Clean up old log backups in incremental folders (and empty folders)
*26) Add Src/Dest Credentials
*27) Add numeric metrics for each type of backup plus errors (full, diff, incremental, failed)
*28) Backup scheduling
*29) Detect VSS error when drive goes offline and recover from the error
 30) Add -CheckDB parameter
*31) Write backups to .dat_ file, and rename it if it completes successfully, otherwise delete it.
*32) Create VSS watchdog on destination servers that will create/delete a snapshot to increase the shadowstorage when necessary


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


.EXAMPLE
PowerBak -FromServer SQLServer1 -FromPath MSSQL:\* -ToServer localhost -ToPath MSSQL: -BackupType Full -CopyOnly

.EXAMPLE
PowerBak -FromServer SQLServer2 -FromPath "MSSQL:SrcInstance\{SELECT Name FROM Sys.Databases WHERE Name NOT IN ('skip1', 'skip2')}" -ToServer localhost -ToPath MSSQL:DestInstance,"C:\MyBackups\Incrementals\" -AppendFromServerFolderToPath -AppendDateTimeFolderToPath

.EXAMPLE
PowerBak -FromServer SQLServer1 -FromPath MSSQL:\* -ToServer localhost -ToPath MSSQL:,"C:\MyBackups\Incrementals\" -AppendFromServerFolderToPath -AppendDateTimeFolderToPath -SinceHours 24

.EXAMPLE
PowerBak -FromServer SQLServer1 -FromPath MSSQL:\* -ToServer localhost -ToPath MSSQL:,"C:\MyBackups\Incrementals\SQLServer1\{[DateTime]::Now.ToString('yyyyMMdd-HHmm')}\" -SinceHours 24

.EXAMPLE
PowerBak -FromServer localhost -FromPath C:\MyBackups\MyDatabase -ToServer SQLServer3 -ToPath MSSQL:NewInstance -AutoRestoreToDateTime "2019-01-14 1:00:00 PM"

.EXAMPLE
PowerBak -FromServer SQLServer2 -FromPath MSSQL:\* -ToServer localhost -ToPath MSSQL:,C:\MyBackups\Incrementals\ -SnapshotManagement @{Volume="Z:"; Expose="C:\MyBackups\Snapshots\"; DaysOfMonth=1,15; DeleteAfterDays=45;} -AppendFromServerFolderToPath -AppendDateTimeFolderToPath

.EXAMPLE
PowerBak -FromServer localhost -FromPath C:\MyBackups\MyDatabase.dat -ToServer localhost -ToPath C:\MyBackups\ -SaveFilesAsBak

.EXAMPLE
PowerBak -BackupJobs C:\PowerBak\AllJobs.config -EmailNotification

.EXAMPLE
PowerBak -BackupJobs C:\PowerBak\AllJobs.config -SinceHours 24

.EXAMPLE
PowerBak -BackupJobs C:\PowerBak\AllJobs.config -ScheduledTask @{TaskName="MyBackup"; StartTime="8:00 PM"; LogonId="NETWORKSERVICE"; LogonType="ServiceAccount"; RunLevel="Highest"; EmailNotification}
#>

#Requires -Version 5.1
[CmdletBinding()]
param
(
    [Parameter(ParameterSetName="FileConfig",Mandatory=$true)][string]$BackupJobs,
    [Parameter(ParameterSetName="FileConfig")][ValidateRange(1, [int]::MaxValue)][int]$ParallelJobs = 0,

    [Parameter(ParameterSetName="M",Position=0,Mandatory=$true)][Parameter(ParameterSetName="FileConfig")][string]$FromServer,
    [Parameter(ParameterSetName="M",Position=1,Mandatory=$true)][Parameter(ParameterSetName="FileConfig")][string]$FromPath,
    [Parameter(ParameterSetName="M",Position=2)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1",Mandatory=$true)][Parameter(ParameterSetName="MaintenanceOnly2",Mandatory=$true)][string]$ToServer = "localhost",
    [Parameter(ParameterSetName="M",Position=3,Mandatory=$true)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1",Mandatory=$true)][string[]]$ToPath,
    [Parameter(ParameterSetName="M",Position=4)][PSCredential]$FromServerCredentials,
    [Parameter(ParameterSetName="M",Position=5)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1")][Parameter(ParameterSetName="MaintenanceOnly2")][PSCredential]$ToServerCredentials,
    [Parameter(ParameterSetName="M",Position=6)][Parameter(ParameterSetName="FileConfig")][ValidateSet("Full", "Differential", "Incremental")][string]$BackupType = "Incremental",
    [Parameter(ParameterSetName="M",Position=7)][Parameter(ParameterSetName="FileConfig")][ValidateSet("Recovery", "NoRecovery", "Standby")][string]$RecoveryType = "Standby",
    [Parameter(ParameterSetName="M",Position=8)][Parameter(ParameterSetName="FileConfig")][switch]$Reverse,
    [Parameter(ParameterSetName="M",Position=9)][Parameter(ParameterSetName="FileConfig")][switch]$SkipWritingDBProperty,
    [Parameter(ParameterSetName="M",Position=10)][Parameter(ParameterSetName="FileConfig")][ValidateRange(0, [int]::MaxValue)][int]$SinceHours = 0,
    [Parameter(ParameterSetName="M",Position=11)][Parameter(ParameterSetName="FileConfig")][ValidateRange(0, [int]::MaxValue)][int]$NetworkTimeout = 60 * 60 * 2,
    [Parameter(ParameterSetName="M",Position=12)][Parameter(ParameterSetName="FileConfig")][ValidateRange(0, [int]::MaxValue)][int]$BufferSize = 1024 * 1024,
    [Parameter(ParameterSetName="M",Position=13)][Parameter(ParameterSetName="FileConfig")][ValidateRange(0, 65535)][int]$TCPPort = 0,
    [Parameter(ParameterSetName="M",Position=14)][Parameter(ParameterSetName="FileConfig")][ValidateRange(0, 22)][int]$CompressionLevel = 6,
    [Parameter(ParameterSetName="M",Position=15)][Parameter(ParameterSetName="FileConfig")][ValidateScript({ $_ % 65536 -eq 0 -and $_ -le 4194304; })][int]$MaxTransferSize = 65536 * 16,
    [Parameter(ParameterSetName="M",Position=16)][Parameter(ParameterSetName="FileConfig")][ValidateRange(0, [int]::MaxValue)][int]$BufferCount = 100,
    [Parameter(ParameterSetName="M",Position=17)][Parameter(ParameterSetName="FileConfig")][ValidateSet("Full", "Bulk_Logged")][string]$SetIncrementalLoggingType = "Bulk_Logged",
    [Parameter(ParameterSetName="M",Position=18)][Parameter(ParameterSetName="FileConfig")][switch]$SkipFilePathOnInitialSeeding,
    [Parameter(ParameterSetName="M",Position=19)][Parameter(ParameterSetName="FileConfig")][switch]$Replace,
    [Parameter(ParameterSetName="M",Position=20)][Parameter(ParameterSetName="FileConfig")][switch]$CopyOnly,
    [Parameter(ParameterSetName="M",Position=21)][Parameter(ParameterSetName="FileConfig")][switch]$ContinueAfterError,
    [Parameter(ParameterSetName="M",Position=22)][Parameter(ParameterSetName="FileConfig")][switch]$ToCheckDB,
    [Parameter(ParameterSetName="M",Position=23)][Parameter(ParameterSetName="FileConfig")][ValidateSet("OnlySeed", "NoSeed", "AllowSeed")][string]$InitialSeeding = "AllowSeed",
    [Parameter(ParameterSetName="M",Position=24)][Parameter(ParameterSetName="FileConfig")][switch]$RandomizeOrder,
    [Parameter(ParameterSetName="M",Position=25)][Parameter(ParameterSetName="FileConfig")][switch]$SaveFilesAsBak,
    [Parameter(ParameterSetName="M",Position=26)][Parameter(ParameterSetName="FileConfig")][string]$SQLDataPath,
    [Parameter(ParameterSetName="M",Position=27)][Parameter(ParameterSetName="FileConfig")][string]$SQLLogPath,
    [Parameter(ParameterSetName="M",Position=28)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1")][Parameter(ParameterSetName="MaintenanceOnly2")][switch]$AppendFromServerFolderToPath,
    [Parameter(ParameterSetName="M",Position=29)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1")][Parameter(ParameterSetName="MaintenanceOnly2")][switch]$AppendDateTimeFolderToPath,
    [Parameter(ParameterSetName="M",Position=30)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1")][ValidateRange(0, 365 * 10)][int]$PurgeBackupHistoryDays = 0,
    [Parameter(ParameterSetName="M",Position=31)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1")][ValidateRange(0, 365 * 10)][int]$DeleteAfterNoBackupRestoreDays = 0,
    [Parameter(ParameterSetName="M",Position=32)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1")][ValidateRange(0, 365 * 10)][int]$DeleteOldFiles = 0,
    [Parameter(ParameterSetName="M",Position=33)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1")][Parameter(ParameterSetName="MaintenanceOnly2",Mandatory=$true)][hashtable]$SnapshotManagement = @{},

    [Parameter(ParameterSetName="M",Position=34)][datetime]$AutoRestoreToDateTime = [DateTime]::MinValue,
    [Parameter(ParameterSetName="M",Position=35)][Parameter(ParameterSetName="MaintenanceOnly1")][Parameter(ParameterSetName="VSSWatchdog")][ValidateRange(0, [int]::MaxValue)][int]$VSSWatchdog = 0L,

    [Parameter(ParameterSetName="M",Position=36)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1")][Parameter(ParameterSetName="MaintenanceOnly2")][hashtable]$ScheduledTask,

    [Parameter(ParameterSetName="M",Position=37)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1")][Parameter(ParameterSetName="MaintenanceOnly2")][switch]$EmailNotification
)

#avoid Strictmode error that $PSCmdlet can't be used because it isn't set (powershell bug ...?)
#$tmpC = $PSCmdlet;
#$PSCmdlet = $tmpC;
$PSCmdlet = $PSCmdlet;

Set-StrictMode -Version 1.0;

Add-Type -Path "$($PSScriptRoot)\lib\AlphaVSS.Common.dll";
Add-Type -Path "$($PSScriptRoot)\lib\VdiDotNet.dll";
Add-Type -Path "$($PSScriptRoot)\lib\EchoStream.NET.dll";
Add-Type -Path "$($PSScriptRoot)\lib\ZStandard.Net.dll";

#since we can create foldernames based on current date/time, we don't want half of some backups in one folder, and half in another, if we strattle an hour.
[datetime]$StartTime = [datetime]::Now;
[hashtable]$shared = $null;
[char]$lastOOB = 0; #keep track of received OOB command when checking for an OOB ack
[string]$gracefulStopMutexName = "Global\PowerBakStop$(Get-Random)"; #needs to be unique in case two different backup jobs are running, so they don't both stop


<##########################
   GENERAL CONFIGURATION
###########################>
#TODO: move to external config file
[string]$autoFolderDateTime = "yyyyMMdd-HHmm";
[string]$jobNotificationFromEmail = "[fill this in]"
[string]$jobNotificationToEmails = "[fill these in]";
[string]$jobNotificationSubject = "PowerBak Report";
[string]$SMTPUsername = "";
[string]$SMTPPassword = "";
[string]$SMTPServer = "[your SMTP IP]";
[int]$SMTPPort = 25;


<##########################
     CLASS DEFINITIONS
###########################>
#metric counts for different activities (most counts will be calculated on the source server, not the destination)
class Metrics
{
    [datetime]$Start;
    [datetime]$End;
    [int]$Full;
    [int]$Differential;
    [int]$Incremental;
    [int]$AutoRestore;
    [int]$Skipped;
    [int]$Reseed;
    [int]$Retry;
    [int]$Success;
    [int]$Failure;
    [int64]$BytesTransferred;
}

#contains all job variables for each copy job, so simultaneous running jobs can be managed
class SyncJob
{
    [Management.Automation.Runspaces.PSSession]$remoteSrc;
    [Management.Automation.Runspaces.PSSession]$remoteDest;
    [Management.Automation.Job]$jobSrc
    [Management.Automation.Job]$jobDest;
    [hashtable]$shared;
    [DatabaseResource]$source;
    [DatabaseResource]$dest;
    [bool]$started;
}

#source/destination resource definition classes
class DatabasePath
{
    [int]$pathType; #0=SQL, 1=File
    [string]$path;
    [string]$namesWildcard; #Filename, Databasename, can contain a wildcard (*), can be just one
}

class DatabaseResource
{
    [string]$server;
    [DatabasePath[]]$paths;

    DatabaseResource([string]$server, [string[]]$paths, [string]$oppositeServer, [bool]$isSource, [bool]$allowAutoAppendFoldersToPath)
    {
        $this.server = $server;
        #$this.paths = [DatabasePath[]]::new($paths.Length);
        $this.paths = [DatabasePath[]]::new(2);
        for($i = 0; $i -lt $paths.Length; $i++)
        {
            if([string]::IsNullOrEmpty($paths[$i])) { continue; } #skip

            $this.paths[$i] = [DatabasePath]::new();
            if($paths[$i].Length -ge 6 -and $paths[$i].Substring(0, 6) -eq "MSSQL:")
            {
                #sql path
                $this.paths[$i].pathType = 0;

                $split = ($paths[$i].Substring(6) -split "\\");
                $this.paths[$i].path = $this.server;
                if(![string]::IsNullOrEmpty($split[0])) { $this.paths[$i].path += ("\" + $split[0]); }

                if($split.Length -lt 2 -or [string]::IsNullOrEmpty($split[1]))
                {
                    if($isSource) { $this.paths[$i].namesWildcard = "*"; }
                }
                else
                {
                    $this.paths[$i].namesWildcard = $split[1];
                }
            }
            else
            {
                #file path
                $this.paths[$i].pathType = 1;

                #expand any expression in path
                $posStart = $paths[$i].IndexOf("{");
                $posEnd = $paths[$i].IndexOf("}");
                if($posStart -gt -1 -and $posEnd -gt -1)
                {
                    $cmd = $paths[$i].Substring($posStart + 1, $posEnd - $posStart - 1);
                    $strReplace = Invoke-Expression $cmd.Replace("[DateTime]::Now", '$($script:StartTime)');
                    $paths[$i] = ($paths[$i].Substring(0, $posStart) + $strReplace + $paths[$i].Substring($posEnd + 1));
                }

                #pull what we think is the filename portion
                #if it is C:\PowerBak\MyBackups then the entire thing is a path
                #if it is C:\PowerBak\MyBackup.bat then the last portion is actually a filename, not part of the path
                #unless we are performing an autorestore, because you can use the syntax: C:\PowerBak\MyBackups\fred, and fred is a database name, not a path
                $this.paths[$i].namesWildcard = [IO.Path]::GetFileName($paths[$i]);
                if([string]::IsNullOrEmpty($this.paths[$i].namesWildcard)) { $this.paths[$i].namesWildcard = "*"; }
                if($script:shared.autoRestoreToDateTime -gt [datetime]::MinValue -or $this.paths[$i].namesWildcard -match "[\*\.]")
                {
                    #if the supposed filename has a wildcard * or dot . then we'll continue to treat it like a filename, so now we can pull the path
                    $this.paths[$i].path = [IO.Path]::GetDirectoryName($paths[$i]);
                }
                else
                {
                    #otherwise, we will treat the entire thing like a folder path
                    $this.paths[$i].path = $paths[$i];
                    $this.paths[$i].namesWildcard = "*"; #which means we want a wildcard * for the filename
                }

                #make sure the path ends with a backslash \
                if($this.paths[$i].path[$this.paths[$i].path.Length - 1] -ne "\") { $this.paths[$i].path += "\"; }

                #add automatic date to path if requested
                if($allowAutoAppendFoldersToPath)
                {
                    if($script:shared.appendFromServerFolderToPath) { $this.paths[$i].path += ($oppositeServer + "\"); }
                    if($script:shared.appendDateTimeFolderToPath -and ![string]::IsNullOrEmpty($script:autoFolderDateTime)) { $this.paths[$i].path += ($script:StartTime.ToString($script:autoFolderDateTime) + "\"); }
                }
            }
        }
    }
}


#database metadata classes
class MetaFile
{
    [int]$type; #0=mdf, 1=ldf, 9=UND (pseudo value we use), -1=used during filecopy
    [string]$logicalName;
    [string]$physicalPath;
    [int64]$size;
    [int64]$used;
}

class MetaData
{
    [string]$name;
    [int]$backupType; #0=full backup, 1=differential, 2=incremental (log), 9=Standby Undo (made up number)
    [int]$compressionLevel;
    [string]$lastLSN;
    [MetaFile[]]$files;
}



<##########################
   FUNCTION DEFINITIONS
###########################>

function WriteErrorEx
{
    param
    (
        [Parameter(Position=0, ParameterSetName="ErrorRecord")][AllowNull()][Management.Automation.ErrorRecord]$err,
        #[Parameter(Position=0, ParameterSetName="AggregateException")][AllowNull()][System.AggregateException]$agg,
        [Parameter(Position=0, ParameterSetName="Exception")][AllowNull()][System.Exception]$ex,
        [Parameter(Position=1)][Threading.Tasks.Task]$task,
        [Parameter(Position=2)][string]$addToNotification
    )

    [string]$errMsg = $addToNotification;
    if($task -ne $null -and $task.Exception -ne $null -and $task.Exception.InnerExceptions -ne $null)
    {
        foreach($exception in $task.Exception.InnerExceptions)
        {
            $errMsg += ($exception.Message+"`r`n"+$exception.StackTrace);
        }
        Write-Error $errMsg;
        return;
    }

    switch($PSCmdlet.ParameterSetName)
    {
        "ErrorRecord"
        {
            if($err -ne $null)
            {
                $errMsg += ($err.Exception.Message+"`r`n"+$err.InvocationInfo.PositionMessage+"`r`n");
                $ex = $err.Exception;
                while($true)
                {
                    $errMsg += ($ex.Message+"`r`n"+$ex.StackTrace+"`r`n");
                    if($ex.InnerException -eq $null) { break; }
                    $ex = $ex.InnerException;
                }
                Write-Error $errMsg;
            }
            else
            {
                Write-Error "ERR IS NULL";
            }

            break;
        }

        "Exception"
        {
            if($ex -ne $null)
            {
                while($ex -ne $null)
                {
                    $errMsg += ($ex.Message+"`r`n"+$ex.StackTrace);
                    $ex = $ex.InnerException;
                }
                Write-Error $errMsg;
            }
            else
            {
                Write-Error "EXCEPTION IS NULL";
            }

            break;
        }

        #"AggregateException"
        #{
        #    if($agg -ne $null -and $agg.InnerExceptions -ne $null)
        #    {
        #        foreach($exception in $agg.InnerExceptions)
        #        {
        #            $errMsg += ($exception.Message+"`r`n"+$exception.StackTrace);
        #        }
        #        Write-Error $errMsg;
        #    }
        #    else
        #    {
        #        Write-Error "AggregateException IS NULL";
        #    }
        #
        #    break;
        #}
    }
}

function WriteErrorAgg
{
    param
    (
        [Parameter(Mandatory=$true)][AllowNull()][System.AggregateException]$err,
        [Parameter()][string]$addToNotification
    )

    [string]$errMsg = $addToNotification;
    if($err -ne $null -and $err.InnerExceptions -ne $null)
    {
        foreach($exception in $err.InnerExceptions)
        {
            $errMsg += ($exception.Message+"`r`n"+$exception.StackTrace);
        }
        Write-Error $errMsg;
    }
    else
    {
        Write-Error "ERR IS NULL";
    }
}

#retrieve database metadata from a stream (most likely network)
function GetMetaData
{
    [OutputType([MetaData])]
    param
    (
        [Parameter(Mandatory=$true)][IO.Stream]$stream
    )

    try
    {
        $b = [byte[]]::new(4);
        $read = $stream.Read($b, 0, $b.Length);
        if($read -ne $b.Length)
        {
            throw "Wrong Number of Bytes Available When Pulling Int32 Size";
        }
        $i = [BitConverter]::ToInt32($b, 0);
        if($i -gt 8192) { throw "Metadata Too Long=$i"; }

        $b = [byte[]]::new($i);
        $read = 0;
        while($read -lt $i)
        {
            $newRead = $stream.Read($b, $read, $b.Length - $read);
            if($newRead -le 0) { throw "Cannot Pull Metadata"; }
            $read += $newRead;
        }
        $json = [Text.Encoding]::UTF8.GetString($b);

        [MetaData]$meta = ConvertFrom-Json $json;
        return $meta;
    }
    catch
    {
        #WriteErrorEx $_;
        throw;
    }
}

#send metadata to a stream (most likely network)
function PutMetaData
{
    param
    (
        [Parameter(Mandatory=$true)][MetaData]$meta,
        [Parameter(Mandatory=$true)][IO.Stream]$stream
    )

    try
    {
        $bMeta = [Text.Encoding]::UTF8.GetBytes((ConvertTo-Json $meta -Compress));
        $b = [BitConverter]::GetBytes($bMeta.Length);
        $stream.Write($b, 0, $b.Length);
        $stream.Write($bMeta, 0, $bMeta.Length);
    }
    catch
    {
        #WriteErrorEx $_;
        throw;
    }
}

#connect to server, execute DBCommand on remote server, and disconnect
#returns the text result of the command executed (not a rowset)
function ExecuteDBCommand
{
    [OutputType([string])]
    param
    (
        [Parameter(Mandatory=$true)][string]$command
    )

    [Net.Sockets.TcpClient]$tcp = $null;
    [IO.Stream]$networkStream = $null;
    try
    {
        $tcp,$networkStream = ConnectToDestinationServer "E$($command)"; #connect to destination server and send DB.Execute command to run

        [int]$totalBytes = -1;
        $startTime = [datetime]::Now;
        $b = [byte[]]::new(4);
        $readTask = $networkStream.ReadAsync($b, 0, $b.Length);
        [string]$DBExecuteResult = $null;
        while(([datetime]::Now - $startTime).TotalMilliseconds -lt $shared.networkTimeout - 1000)
        {
            if($readTask.IsCompleted -and $totalBytes -eq -1)
            {
                if($readTask.Result -ne $b.Length)
                {
                    throw "Wrong number of bytes ($($readTask.Result)) available when getting result for ExecuteDBCommand($($command))";
                }

                $totalBytes = [BitConverter]::ToInt32($b, 0);
                if($totalBytes -lt 0 -or $totalBytes -gt 1024 * 1024 * 10)
                {
                    throw "DBExecute result too long [$i]";
                }

                if($totalBytes -gt 0)
                {
                    $b = [byte[]]::new($totalBytes);
                    $read = 0;
                    while($read -lt $totalBytes)
                    {
                        $newRead = $networkStream.Read($b, $read, $b.Length - $read);
                        if($newRead -le 0) { throw "Cannot read DBExecute result"; }
                        $read += $newRead;
                    }
                    $DBExecuteResult = [Text.Encoding]::UTF8.GetString($b);
                }
            }

            $OOBCmd = GetOOBCommand $tcp;
            if($OOBCmd -ne 0) { break; }

            Start-Sleep -Milliseconds 100;
        }

        if($readTask.IsCompleted) { $readTask.Dispose(); }
        $readTask = $null;

        if($OOBCmd -ne 'D')
        {
            throw "Error running DB.Execute";
        }

        return $DBExecuteResult;
    }
    finally
    {
        #clean up connection
        if($tcp -ne $null)
        {
            CloseObjects $tcp $networkStream;
            $tcp = $null;
            $networkStream = $null;
        }
    }
}

#set up TCP Listenter, and determine dynamic port number if it is 0
function CreateTCPListener
{
    [OutputType([int])]
    param
    (
        [Parameter(Mandatory=$true)][Net.IPAddress]$address,
        [Parameter(Mandatory=$true)][int]$port
    )

    try
    {
        Write-Verbose "Setting up TCPListener";
        [Net.Sockets.TCPListener]$listener = $null;
        if($port -eq 0)
        {
            $rnd = [Random]::new();
            for($i = 0; $i -lt 1000; $i++)
            {
                $port = $rnd.Next(49152, 65535); #IANA suggsted dynamic port range
                $endpoint = [Net.IPEndPoint]::new($address, $port);
                $listener = [Net.Sockets.TcpListener]::new($endpoint);
                try
                {
                    $listener.Start();
                    break;
                }
                catch
                {
                    #port in use, try next one
                }
            }
            $shared.TCPPort = $port; #set port found
        }
        else
        {
            $endpoint = [Net.IPEndPoint]::new($address, $port);
            $listener = [Net.Sockets.TcpListener]::new($endpoint);
            $listener.Start();
        }
        Write-Verbose "Listening On $($shared.address):$($shared.TCPPort)";
        $script:listener=$listener;

        return $port;
    }
    catch
    {
        #WriteErrorEx $_;
        throw;
    }
}

#TCP listener loop, handle connections after calling CreateTCPListener
function ProcessTCPConnections
{
    param
    (
        [Parameter(Mandatory=$true)][ScriptBlock]$ScriptBlock
    )
    try
    {
        $startTime = [datetime]::Now;
        while(([datetime]::Now - $startTime).TotalMilliseconds -lt $shared.networkTimeout + (60 * 1000)) #timeout plus 1 minute
        {
            if($listener.Pending())
            {
                #Start-Sleep -Milliseconds 10;
                $tcp = $listener.AcceptTcpClient();

                $script:lastOOB = 0; #reset last OOB for new connections

                $tcp.ReceiveTimeout = $shared.networkTimeout;
                $tcp.SendTimeout = $shared.networkTimeout;
                $tcp.SendBufferSize = $shared.bufferSize;
                $tcp.ReceiveBufferSize = $shared.bufferSize;
                $tcp.NoDelay = $true;
                $tcp.Client.SendBufferSize = $shared.bufferSize;
                $tcp.Client.ReceiveBufferSize = $shared.bufferSize;
                $tcp.Client.SendTimeout = $shared.networkTimeout;
                $tcp.Client.ReceiveTimeout = $shared.networkTimeout;
                $tcp.Client.NoDelay = $true;
                $tcp.Client.DontFragment = $true;
                $null = SetKeepAlive $tcp.Client;

                $ep = [Net.IPEndPoint]$tcp.Client.RemoteEndPoint;

                $lingerOption = [Net.Sockets.LingerOption]::new($true, 5);
                $tcp.LingerState = $lingerOption;
                $tcp.Client.LingerState = $lingerOption;
                #$tcp.Client.SetSocketOption([Net.Sockets.SocketOptionLevel]::Socket, [System.Net.Sockets.SocketOptionName]::Linger, $lingerOption);

                Write-Verbose "$($source.paths[0].path): TCP Connection Established From $($ep.Address):$($ep.Port)";

                #run this script for each TCP connection
                $networkStream = $tcp.GetStream();
                try
                {
                    $shouldExit = & $ScriptBlock $tcp $networkStream;
                }
                catch
                {
                    WriteErrorEx $_;
                }

                Write-Verbose "$($source.paths[0].path): ProcessTCPConnections() - Closing TCP From $($ep.Address):$($ep.Port)";
                CloseObjects $tcp $networkStream;
                $tcp = $null;
                $networkStream = $null;

                if($shouldExit -eq $true)
                {
                    Write-Information "Done Processing: $($source.paths[0].path)";
                    break;
                }

                #reset timer
                $startTime = [datetime]::Now;
            }
            else
            {
                Start-Sleep -Milliseconds 100;
            }
        }
    }
    catch
    {
        WriteErrorEx $_;
        #throw;
    }
    finally
    {
        Write-Verbose "$($source.paths[0].path): Stopping listener";
        try { $listener.Stop(); } catch {}
    }
}

#close network objects
function CloseObjects
{
    param
    (
        [Parameter()][Net.Sockets.TcpClient]$tcp,
        [Parameter()][Net.Sockets.NetworkStream]$networkStream,
        [Parameter()][VdiDotNet.VdiEngine]$vdi,
        [Parameter()][System.Threading.Tasks.Task]$vdiTask,
        [Parameter()][Zstandard.Net.ZstandardStream]$zstd
   )

    try
    {
        if($networkStream -ne $null)
        {
            #$networkStream.Flush();
            #$networkStream.Close();
        }

        if($tcp -ne $null)
        {
            if($tcp.Client.Connected)
            {
                try { $tcp.Client.Shutdown([System.Net.Sockets.SocketShutdown]::Receive); } catch {}
                #try { $tcp.Client.Disconnect($false); } catch {}
            }
        }

        if($networkStream -ne $null)
        {
            #$networkStream.Dispose();
            $networkStream = $null;
        }

        if($vdiTask -ne $null)
        {
            #make sure the VDI task has finished
            if(!$vdiTask.IsCompleted)
            {
                Write-Verbose "$($source.paths[0].path): VDITask isn't stopped, giving it additional time";
                $tasks = [Threading.Tasks.Task[]]::new(1);
                $tasks[0] = $vdiTask;
                #was $shared.networkTimeout instead of 5000
                if([System.Threading.Tasks.Task]::WaitAny($tasks, 5000) -eq -1) { Write-Error "Database Backup Error: $($SQLServer) -> Cannot Abort VDI Task!?"; }
            }
        }

        if($vdiTask -ne $null)
        {
            $vdiTask.Dispose();
            $vdiTask = $null;
        }

        if($vdi -ne $null)
        {
            if($zstd -ne $null -and $vdi.VDIStream -eq $zstd)
            {
                $vdi.DisposeVDIStreamAfterReadWriteTask = $true;
                $zstd = $null;
            }
            $vdi.Dispose();
            $vdi = $null;
        }

        if($zstd -ne $null)
        {
            #Write-Verbose "$($source.paths[0].path): Closing compression";
            try { $zstd.Close(); } catch { }
            $zstd.Dispose();
            $zstd = $null;
        }

        if($tcp -ne $null)
        {
            #$tcp.Close();
            #$tcp.Dispose();
            $tcp = $null;
        }
    }
    catch
    {
        WriteErrorEx $_ -addToNotification "::CloseObjects() - "
    }
}

#connect to TCP server to process a database
function ConnectToDestinationServer
{
    [OutputType([Net.Sockets.TcpClient],[IO.Stream])]
    param
    (
        [Parameter(Mandatory=$true)][string]$command,
        [Parameter()][MetaData]$meta,
        [Parameter()][ref]$abort
    )

    try
    {
        Write-Verbose "$($source.paths[0].path): Begin TCP Connection To $($shared.address):$($shared.TCPPort)";

        $script:lastOOB = 0; #reset last OOB for new connections

        $tcp = [Net.Sockets.TcpClient]::new($shared.address, $shared.TCPPort);
        $tcp.SendBufferSize = $shared.bufferSize;
        $tcp.ReceiveBufferSize = $shared.bufferSize;
        $tcp.SendTimeout = $shared.networkTimeout;
        $tcp.ReceiveTimeout = $shared.networkTimeout;
        $tcp.NoDelay = $true;
        $tcp.Client.SendBufferSize = $shared.bufferSize;
        $tcp.Client.ReceiveBufferSize = $shared.bufferSize;
        $tcp.Client.SendTimeout = $shared.networkTimeout;
        $tcp.Client.ReceiveTimeout = $shared.networkTimeout;
        $tcp.Client.NoDelay = $true;
        $tcp.Client.DontFragment = $true;
        $null = SetKeepAlive $tcp.Client;

        $lingerOption = [Net.Sockets.LingerOption]::new($true, 5);
        $tcp.LingerState = $lingerOption;
        $tcp.Client.LingerState = $lingerOption;
        #$tcp.Client.SetSocketOption([Net.Sockets.SocketOptionLevel]::Socket, [Net.Sockets.SocketOptionName]::Linger, $lingerOption);

        $stream = $tcp.GetStream();
        Write-Verbose "$($source.paths[0].path): TCP Connection Established To $($shared.address):$($shared.TCPPort)";

        #wait for destination to let us know they are ready
        $tcp.Client.ReceiveTimeout = 60 * 1000 * 5; #lower to 5 minute timeout
        $b = [byte[]]::new(1);
        $null = $stream.Read($b, 0, $b.Length);
        Write-Verbose "$($source.paths[0].path): Destination Is Ready=$($b[0])";
        $tcp.Client.ReceiveTimeout = $shared.networkTimeout; #put timeout back

        if($abort -ne $null)
        {
            if($b[0] -eq 66) #66 = abort requested at earliest convenience, 65 = good to go
            {
                $abort.Value = $true;
            }
            else
            {
                $abort.Value = $false;
            }
        }

        #send security string
        Write-Verbose "$($source.paths[0].path): Send Security String";
        $b = $shared.security.ToByteArray();
        $stream.Write($b, 0, $b.Length);

        #send command
        $bCommand = [Text.Encoding]::UTF8.GetBytes($command);
        $b = [BitConverter]::GetBytes($bCommand.Length);
        $stream.Write($b, 0, $b.Length);
        $stream.Write($bCommand, 0, $bCommand.Length);

        #send meta data
        if($meta -ne $null)
        {
            PutMetaData $meta $stream;
        }


        return $tcp,$stream;
    }
    catch
    {
        WriteErrorEx $_;
        if($abort -ne $null) { $abort.Value = $true; }
        throw;
    }
}

#Socket keepalive
function SetKeepAlive
{
    [OutputType([bool])]
    param
    (
        [Parameter(Mandatory=$true)][Net.Sockets.Socket]$sock,
        [Parameter()][uint32]$keepAliveTime = 60 * 1000, #60 second timeout
        [Parameter()][uint32]$keepAliveInterval = 60 * 1000 #60 second retry after timeout (60 seconds + (60 seconds * 10 retries) = 660 seconds of no response)
    )

    try
    {
        #keepalive input values
        $input = [uint32[]]::new(3);
        if($keepAliveTime -eq 0 -or $keepAliveInterval -eq 0) # enable disable keep-alive
        {
            $input[0] = 0;
        }
        else
        {
            $input[0] = 1;
        }
        $input[1] = $keepAliveTime; # time millis
        $input[2] = $keepAliveInterval; # interval millis

        # pack input into byte struct
        $SIO_KEEPALIVE_VALS = [byte[]]::new(3 * 4);
        for($i = 0; $i -lt $input.Length; $i++)
        {
            $SIO_KEEPALIVE_VALS[$i * 4 + 3] = [byte](($input[$i] -shr 24) -band 0xff);
            $SIO_KEEPALIVE_VALS[$i * 4 + 2] = [byte](($input[$i] -shr 16) -band 0xff);
            $SIO_KEEPALIVE_VALS[$i * 4 + 1] = [byte](($input[$i] -shr 8) -band 0xff);
            $SIO_KEEPALIVE_VALS[$i * 4 + 0] = [byte](($input[$i] -shr 0) -band 0xff);
        }

        # write SIO_VALS to Socket IOControl
        $null = $sock.IOControl([Net.Sockets.IOControlCode]::KeepAliveValues, $SIO_KEEPALIVE_VALS, $null);
    }
    catch
    {
        WriteErrorEx $_;
        return $false;
    }
    return $true;
}

#Send OOB Command Over TCP Socket (will not show up in normal stream)
#if the command is not a success, it will flush the incoming socket, so the ACK packet will not get stuck
function SendOOBCommand
{
    [OutputType([bool])]
    param
    (
        [Parameter()][Net.Sockets.TcpClient]$tcp,
        #[Parameter()][Net.Sockets.NetworkStream]$stream,
        [Parameter(Mandatory=$true)][char]$command,
        [Parameter()][bool]$forceFlushIncomingData
    )

    #if the source/destination are on the same machine and one side disconnects the tcp socket,
    #sending an OOB can cause the network to timeout instead of immediately throw an error
    #therefore, to prevent this, we need to set the timeout to just a few seconds for the duration of sending the OOB command.
    #on the way out, we will set the timeout back to what it was

    #$script:lastOOB = 0; #reset last OOB when sending a new command

    $counter = 0;
    try
    {
        $b = [byte[]]::new(1);
        $b[0] = [byte]$command;

        while($true)
        {
            if($forceFlushIncomingData -or ($command -notin 'D','X'))
            {
                #if not sending a success message, flush incoming bytes, so the OOB ACK doesn't get stuck from the other end
                if($tcp.Client.Available -gt 0)
                {
                    $holdTimeout = $tcp.Client.ReceiveTimeout;
                    $tcp.Client.ReceiveTimeout = 1000;

                    $stream = $tcp.GetStream();
                    $dummy = [byte[]]::new($tcp.Client.Available);
                    Write-Verbose "$($source.paths[0].path): Flushing $($dymmy.Length) incoming bytes before sending OOB Command";
                    try { $null = $stream.Read($dummy, 0, $dummy.Length); } catch {}

                    $tcp.Client.ReceiveTimeout = $holdTimeout;
                }
            }

            if($counter % 1000 -eq 0)
            {
                Write-Verbose "$($source.paths[0].path): Sending OOB Command: $($command)$(if($counter -gt 0) { " : Retry, read bytes stuck=$($tcp.Client.Available)" })";

                Start-Sleep -Milliseconds 100; #pre OOB
                $b[0] = [byte]$command;
                if($tcp.Client.Send($b, 1, [Net.Sockets.SocketFlags]::OutOfBand) -ne 1)
                {
                    Write-Error "$($source.paths[0].path): Error cannot send OOB Command: $($command)$(if($counter -gt 0) { " : Retry, read bytes stuck=$($tcp.Client.Available)" })";
                    break;
                }
            }
            $counter++;

            if(!$tcp.Client.Connected) { break; }

            Start-Sleep -Milliseconds 100;

            [char]$ack = 0;
            $ack = GetOOBCommand $tcp -checkAcknowledgement $true;
            if($ack -ne 0)
            {
                Write-Verbose "$($source.paths[0].path): Sent OOB Command: $command, Received ACK: $ack";
                return $true;
            }
        }
    }
    catch
    {
        #WriteErrorEx $_
    }
    #finally
    #{
    #    $tcp.Client.SendTimeout = $holdTimeout;
    #}

    if($counter -eq 0)
    {
        Write-Information "$($source.paths[0].path): Error Sending OOB Command: $($command)";
        return $false;
    }
    else
    {
        Write-Information "$($source.paths[0].path): Sent OOB Command: $command, Did NOT Receive ACK Before Far Side Dropped";
        return $true;
    }
}

#Wait until an OOB Command From TCP Socket is returned (or return 0 if disconnected)
#used if we need to actively wait for an expected OOB from the other side, faster than disconnecting a socket and letting the other side eventually figure it out
function WaitOOBCommand
{
    [OutputType([char])]
    param
    (
        [Parameter(Mandatory=$true)][Net.Sockets.TcpClient]$tcp
    )

    try
    {
        Write-Verbose "$($source.paths[0].path): Waiting for OOB Command...";

        [char]$OOBCmd = 0;
        $startTime = [datetime]::Now;
        while(([datetime]::Now - $startTime).TotalMilliseconds -lt $shared.networkTimeout -and $tcp.Connected)
        {
            #make sure we don't have the pipe completely clogged, or else we won't ever be able to receive the OOB
            if($tcp.Client.Available -gt 0)
            {
                Write-Verbose "$($source.paths[0].path): Flushing incoming bytes then waiting again for OOB";

                $holdTimeout = $tcp.Client.ReceiveTimeout;
                $tcp.Client.ReceiveTimeout = 1000;

                $stream = $tcp.GetStream();
                $dummy = [byte[]]::new($tcp.Client.Available);
                try { $null = $stream.Read($dummy, 0, $dummy.Length); } catch {}

                $tcp.Client.ReceiveTimeout = $holdTimeout;
            }

            $OOBCmd = GetOOBCommand $tcp;
            #always sleep 100ms, that way the return ACK has time to get across
            Start-Sleep -Milliseconds 100;

            if($OOBCmd -ne 0) { break; }
        }

        return $OOBCmd;
    }
    catch
    {
        throw;
    }
}

#Retrieve an OOB Command From TCP Socket
function GetOOBCommand
{
    [OutputType([char])]
    param
    (
        [Parameter(Mandatory=$true)][Net.Sockets.TcpClient]$tcp,
        [Parameter()][bool]$checkAcknowledgement = $false
    )

    try
    {
        if(!$checkAcknowledgement -and $script:lastOOB -ne 0)
        {
            $ret = $script:lastOOB;
            Write-Verbose "$($source.paths[0].path): Return last OOB: $($ret)";
            $script:lastOOB = 0;

            return $ret;
        }

        [byte[]]$b = [BitConverter]::GetBytes([int]0);
        $ioBytes = $tcp.Client.IOControl([Net.Sockets.IOControlCode]::OobDataRead, $null, $b);
        #if($tcp.Client.IOControl([Net.Sockets.IOControlCode]::OobDataRead, $null, $b) -gt 0)
        if($ioBytes -eq $b.Length)
        {
            $availOOBCount = [BitConverter]::ToInt32($b, 0);
            if($availOOBCount -eq 0)
            {
                #data available
                #Write-Verbose "$($source.paths[0].path): OOB DATA AVAILABLE COUNT=$($availOOBCount) IOControl bytecount=$($ioBytes)";

                Start-Sleep -Milliseconds 100; #pre OOB
                $b[0]=0;
                if($tcp.Client.Receive($b, 1, [Net.Sockets.SocketFlags]::OutOfBand) -eq 1)
                {
                    if([char]$b[0] -notin ('A','B','D','E','F','P','R','S','T','U','X','Z'))
                    {
                        Write-Error "$($source.paths[0].path): BAD OOB RECEIVED: $($b[0]) !?!?";
                        return 0;
                    }

                    #Write-Verbose "$($source.paths[0].path): OOB DATA! = $([char]$b[0])";
                    if(!$checkAcknowledgement)
                    {
                        if($b[0] -eq [char]'Z')
                        {
                            #do not return an ACK unless we are checking for one, shouldn't ever happen
                            Write-Error "$($source.paths[0].path): RECEIVED OOB Z UNEXPECTEDLY!";
                            return 0;
                        }

                        #send acknowledgement
                        $ack = [byte[]]::new(1);
                        $ack[0] = [char]'Z';
                        try
                        {
                            Start-Sleep -Milliseconds 100; #pre ACK
                            if($tcp.Client.Send($ack, 1, [Net.Sockets.SocketFlags]::OutOfBand) -eq 1)
                            {
                                Start-Sleep -Milliseconds 100; #give ACK time to send
                                #$null = $sock.IOControl([Net.Sockets.IOControlCode]::Flush, $SIO_KEEPALIVE_VALS, $null);

                                Write-Verbose "$($source.paths[0].path): Received OOB Command: $([char]$b[0]) and ACK'd it";
                                return [char]$b[0];
                            }
                        }
                        catch {}
                        Write-Error "$($source.paths[0].path): Received OOB Command: $([char]$b[0]) but could NOT ACK it";
                        return [char]$b[0];
                    }
                    else
                    {
                        if($b[0] -ne [char]'Z')
                        {
                            #store last received OOB, we'll return this when not checking for an ACK
                            $script:lastOOB = [char]$b[0];
                            Write-Verbose "$($source.paths[0].path): Set last good OOB=$($script:lastOOB) and send ACK";

                            #send acknowledgement
                            $ack = [byte[]]::new(1);
                            $ack[0] = [char]'Z';
                            try
                            {
                                Start-Sleep -Milliseconds 100; #pre ACK
                                if($tcp.Client.Send($ack, 1, [Net.Sockets.SocketFlags]::OutOfBand) -eq 1)
                                {
                                    Start-Sleep -Milliseconds 100; #give ACK time to send
                                    Write-Verbose "$($source.paths[0].path): Stashed last good OOB Command: $([char]$b[0]) and ACK'd it";
                                    return 0;
                                }
                            }
                            catch {}
                            Write-Error "$($source.paths[0].path): Stashed last good OOB Command: $([char]$b[0]) but could NOT ACK it";
                            return [char]'Z'; #return an ACK, since we could not send an ack, otherwise we'll get stuck waiting forever
                        }
                        else
                        {
                            Write-Verbose "$($source.paths[0].path): Received OOB ACK";
                            return [char]$b[0];
                        }
                    }
                }
                else
                {
                    Write-Error "$($source.paths[0].path): Could Not Read OOB!?!?";
                }
            }
        }
        else
        {
            Write-Error "$($source.paths[0].path): Could Not Check OOB Bytes Available!?!?";
        }
        return 0;
    }
    catch
    {
        WriteErrorEx $_;
        return 0;
    }
}

#estimate file size based on metadata
function EstimateFileSize
{
    [OutputType([int64])]
    param
    (
        [Parameter(Mandatory=$true)][MetaData]$meta,
        [Parameter()][ref]$isCompressed = $null
    )

    [int64]$fileSize = 0L;
    $isFileCopy = $false;
    try
    {
        if($isCompressed -ne $null) { $isCompressed.Value = $false; }
        [MetaFile]$f = $null;
        foreach($f in $meta.files)
        {
            #add size of MDF/LDF for full back, or just LDF for incremental/differential
            #or if sending a regular file (type = -1), add the size
            #TODO: differential backup will not be the size of the LDF, so this value will be incorrect
            if($meta.backupType -eq 0 -or $f.type -eq 1 -or $f.type -eq -1)
            {
                $fileSize += [int64]$f.used; #size in bytes
                if($f.type -eq -1) { $isFileCopy = $true; }
            }
        }
        if(!$isFileCopy -and $meta.compressionLevel -gt 0 -and !$shared.saveFilesAsBak)
        {
            #if compression is on, halve the estimated bytes? Should be close
            if($isCompressed -ne $null) { $isCompressed.Value = $true; }
            $fileSize = [int64]($fileSize / 2L * 1.25);
        }
    }
    catch
    {
        WriteErrorEx $_ -addToNotification "ExtimateFileSize -> ";
    }
    return $fileSize;
}

#Copy from one source stream to one or more destination streams ... until there's nothing left
#When watchTask != null, exit only when the task is complete or if an OOB failure message is received.
#When watchTask == null, exit only when 0 (or less) bytes are read or if an OOB failure message is received, or if an OOB success message is received and there hasn't been any data for a period of time.
#All streaming copy operations use this function, except for BackupDatabsae, which writes into the stream directly from the VDI
# Return values:
# 0=success (OOB received)
# 1=success (no OOB received)
# 2=success, and do not continue backuping up (OOB X received)
# 10=fail (OOB received)
# 11=fail (no OOB received)
# 12=retry without RESTART option (OOB received)
# 13=retry with CONTINUE_AFTER_ERROR option (OOB received)
# 99=skip (OOB received)
#
# There are five ways this function can exit.
# 1) The stream returns <= 0 bytes on a successful read (return 1)
# 2) The optional TcpClient receives an OOB command of 'D'one (return 0), 'A'bort (return 10), or Ski'P' (return 99)
# 3) The optional watchTask completes (return 2, the task result will still need to be checked by the caller for any errors.. just because the stream completed doesn't mean the task was successful)
# 4) A networkTimeout period elapses without receiving any data (return 11)
# 5) An error is thrown, which is most likely a stream error (return 11)
function CopyStream
{
    [OutputType([int])]
    param
    (
        [Parameter(Mandatory=$true)][IO.Stream]$readStream,
        [Parameter(Mandatory=$true)][IO.Stream[]]$writeStreams,
        [Parameter()][Net.Sockets.TcpClient]$tcp = $null, #check for OOB notification when stream is done
        [Parameter()][Threading.Tasks.Task[Exception]]$watchTask = $null, #watch task hooked to destination stream to see if it is done
        [Parameter()][MetaData]$meta = $null, #optional, used to get things like databasename and calculate total bytes
        [Parameter()][VdiDotNet.VdiEngine]$vdi = $null #optional, used to get percent complete
    )

    [int]$progressId = Get-Random -Minimum 1 -Maximum 1000000;
    [int64]$readTotal = 0L;
    [char]$OOBCmd = 0;
    [bool]$readEstimatedCompressed = $false;
    [string]$progressActivity = $null;
    $start = [DateTime]::Now;
    try
    {
        #find destination database name, it's the same as the source (meta.name) unless an explicit destination name was specified in the command line
        [string]$destDBName = $null;
        if($dest.paths[0].pathType -eq 0 -and ![string]::IsNullOrEmpty($dest.paths[0].namesWildcard))
        {
            $destDBName = $dest.paths[0].namesWildcard;
        }
        elseif($dest.paths.length -gt 1 -and $dest.paths[1].pathType -eq 0 -and ![string]::IsNullOrEmpty($dest.paths[1].namesWildcard))
        {
            $destDBName = $dest.paths[1].namesWildcard;
        }
        else
        {
            #if a destination name was not explicitly specified, fall back to the source meta.name
            $destDBName = $meta.name;
        }
         
        $progressActivity = "$($start.ToString()): $($source.server)\$($meta.name) -> $($dest.server)\$($destDBName)";
        [int64]$readProgress = 1024L * 1024L * 100L; #report progress every time this boundary is crossed (100MB)
        [int64]$readEstimated = 0L;
        [string]$estimatedMB = $null;

        if($shared.isDestination -and $meta -ne $null) #only show progress meter on destination
        {
            $readEstimated = EstimateFileSize $meta ([ref]$readEstimatedCompressed);
            if($readEstimated -eq 0L) { $readEstimated = 1L; } #prevent divide by 0 error

            $estimatedMB = "$(([int64][Math]::Floor($readEstimated / 1024L / 1024L)).ToString("N0")) MB";
            Write-Progress $progressActivity "0 MB / $($estimatedMB)" -Id $progressId -PercentComplete 0;

            for($i = 0; $i -lt $writeStreams.Length; $i++)
            {
                if($writeStreams[$i].GetType() -eq [IO.FileStream])
                {
                    Write-Verbose "$($dest.server)\$($meta.name): Setting initial file length to $($readEstimated)";
                    $writeStreams[$i].SetLength($readEstimated);
                }
            }
        }

        $b = [byte[][]]::new(2); #create two buffer arrays, so they can read/write simultaneously, then swap places
        $b[0] = [byte[]]::new($shared.bufferSize);
        $b[1] = [byte[]]::new($shared.bufferSize);

#test
#Write-Verbose "$($source.paths[0].path): write streams count = $($writeStreams.Length)";

        $writeTasks = [Threading.Tasks.Task[]]::new($writeStreams.Length);
        for($i = 0; $i -lt $writeStreams.Length; $i++)
        {
            $writeTasks[$i] = [Threading.Tasks.Task]::CompletedTask;
        }

        $usebuf = 0;
        $lastRead = [DateTime]::Now;
        $readProgressPrevious = 0;
        $percentComplete = 0.0;
        $percentCompletePrevious = 0.0;
        $remainingSeconds = -1;
        $readReady = $false;
        $writeReady = $false;
        [Threading.Tasks.Task[int]]$readTask = $null;

        #copy loop
        while($true)
        {
            if($readTask -eq $null)
            {
#test
#Write-Verbose "$($source.paths[0].path): setup read";
                #setup new read
                $readTask = $readStream.ReadAsync($b[$usebuf], 0, $b[$usebuf].Length);
            }
#test
#Write-Verbose "$($source.paths[0].path): wait 100ms for read task";
            $readReady = $readTask.Wait(100);
#Write-Verbose "readready=$($readReady), writeready=$($writeReady) writetask0=$($writeTasks[0].IsCompleted) vdi.readTask=$($vdi.ReadTask.IsCompleted) vdi.writeTask=$($vdi.WriteTask.IsCompleted) watchtask=$($watchTask.IsCompleted) readTotal=$($readTotal)";
#if($watchTask.IsCompleted) { Write-Verbose "($($watchTask.Result.Message))" }

            if(!$readReady -and $OOBCmd -ne 0 -and $watchTask -eq $null)
            {
                #if we receive a 'D' or 'X' OOB command, check one more time here and keep looping if we still have more data coming in
                #it will exit if/when the remote side closes the TCP connection, unless it's waiting for us to confirm by sending a 'D' back
                #so we can't wait forever, but hopefully two of these in a row is enough
                #TODO: we need to find a better way to keep looping while making sure we get the final bit of data
                $readReady = $readTask.Wait(500);
#test
#Write-Verbose "Final readReady Check: $($readReady) bytesTransferred: $($readTotal)";
            }

            $writeReady = [Threading.Tasks.Task]::WaitAll($writeTasks, 100);

            if($tcp -ne $null -and $OOBCmd -eq 0)
            {
                $OOBCmd = GetOOBCommand $tcp;
                if($OOBCmd -ne 0 -and $OOBCmd -ne 'D' -and $OOBCmd -ne 'X')
                {
                    #if we receive an OOB failure message, exit early
                    break;
                }
                #if we receive a 'D' or 'X' OOB command, loop until the read/write tasks are both completed (or the watchTask is completed, which is SQL telling us it's done)
            }

            if($readReady -and $writeReady -and $readTask.Result -gt 0)
            {
                #a new read is ready, and the previous writes are complete, so we can safely swap buffers

                if($shared.isDestination)
                {
                    #pause here if the VSS needs to refill the diff space, will reduce I/O load to make it easier to refill without losing the snapshot
                    CheckVSSWatchDog;
                }

                $lastRead = [DateTime]::Now; #reset timeout

                $read = $readTask.Result;
                $readTask.Dispose();
                $readTask = $null;

#test
#Write-Verbose "$($source.paths[0].path): xfer:$read";

                for($i = 0; $i -lt $writeStreams.Length; $i++)
                {
                    #if($writeTasks[$i] -ne [Threading.Tasks.Task]::CompletedTask) { $writeTasks[$i].Dispose(); }
                    $writeTasks[$i] = $writeStreams[$i].WriteAsync($b[$usebuf], 0, $read); #kick off new write to the other stream
                }
#test
#Write-Verbose "$($source.paths[0].path): swap buffers";
                $usebuf = if($usebuf) { 0 } else { 1 }; #swap between buffers

                if(!$shared.isDestination) #only show progress meter on destination
                {
                    $readTotal += $read; #keep running byte transfer total
                }
                else
                {
                    #calculate progress
                    $readProgressPrevious = [int64][Math]::Floor($readTotal / $readProgress);
                    $readTotal += $read; #keep running byte transfer total
                    $readProgressCurrent = [int64][Math]::Floor($readTotal / $readProgress);
                    if($readProgressPrevious -lt $readProgressCurrent)
                    {
                        $currentMB = "$(([int64][Math]::Floor($readTotal / 1024L / 1024L)).ToString("N0")) MB";
                        if($meta -eq $null)
                        {
                            #simple progress
                            Write-Verbose "$($progressActivity) : $($currentMB) / $($estimatedMB)";
                        }
                        else
                        {
                            #progress meter (requires meta object)
                            if($false) #$vdi -ne $null)
                            {
                                #get exact complete if we do have a vdi object
                                $percentComplete = $vdi.PercentComplete;
                                if($percentComplete -ne $percentCompletePrevious)
                                {
                                    if($percentComplete -le 0.0) { $percentComplete = .01; } #prevent divide by 0 error
                                    if($percentComplete -gt 100.0) { $percentComplete = 100.0; } #prevent Write-Progress > 100 percent error
                                    $percentCompletePrevious = $percentComplete;
                                    $currentSeconds = ([DateTime]::Now - $start).TotalSeconds;
                                    $secondsEachPercent = $currentSeconds / $percentComplete;
                                    $remainingSeconds = $secondsEachPercent * (100.0 - $percentComplete);
                                }
                            }
                            else
                            {
                                #calculate percent complete if we don't have a vdi object
                                if($readTotal -ge $readEstimated)
                                {
                                    $percentComplete = 100.0;
                                }
                                else
                                {
                                    $percentComplete = $readTotal / $readEstimated * 100.0;
                                }
                                if($percentComplete -le 0.0) { $percentComplete = .01; } #prevent divide by 0 error
                                if($percentComplete -gt 100.0) { $percentComplete = 100.0; } #prevent Write-Progress > 100 percent error
                                $currentSeconds = ([DateTime]::Now - $start).TotalSeconds;
                                $secondsEachPercent = $currentSeconds / $percentComplete;
                                $remainingSeconds = $secondsEachPercent * (100.0 - $percentComplete);
                            }
                            Write-Progress $progressActivity "$($currentMB) / $($estimatedMB)" -Id $progressId -PercentComplete ([int]$percentComplete) -SecondsRemaining ([int]$remainingSeconds);
                        }
                    }
                }
            }
            else
            {
                #Write-Verbose "$($source.paths[0].path): read/write not ready, check to see if we need to exit";

                #if we have a watchTask, see if it has completed
                if($watchTask -ne $null)
                {
                    if($watchTask.IsCompleted)
                    {
                        if($tcp -eq $null)
                        {
                            #I don't think this can happen, having a watchTask without a tcp socket
                            Write-Error "A Unicorn was just born in CopyStream";
                            return 1;
                        }

#Write-Verbose "$($source.paths[0].path): CopyStream WatchTask Copmleted";
                        if($OOBCmd -ne 0)
                        {
                            #if the watchtask is complete (i.e. SQL is done with restore), and we have received an OOB code, return, should be successful
                            #break out of the loop and return based on the OOB
                            break;
                        }
                        else
                        {
                            return 1; #success, no OOB received
                        }
                    }
                }
                else
                {
                    #if there isn't a watchtask, check that the current write has completed
                    if($writeReady)
                    {
                        #if the read is complete and we have received 0 (or less) bytes
                        if($readReady -and $readTask.Result -le 0)
                        {
#test
#Write-Verbose "$($source.paths[0].path): CopyStream read 0 or less bytes: $($read)";

                            #if the bytes read is 0 or less, we are done reading
                            #if we have not received an OOB, only return success if the read bytes is exactly 0
                            if($OOBCmd -eq 0 -and $readTask.Result -eq 0)
                            {
                                return 1; #success, no OOB received
                            }

                            #break out of the loop and return based on the OOB (or lack thereof)
                            break;
                        }
                        else
                        {
                            #if we have received an OOB, and more than 0 bytes have been copied
                            if($OOBCmd -ne 0  -and $readTotal -gt 0L)
                            {
                                #break out of the loop and return based on the OOB (or lack thereof)
#Write-Verbose "$($source.paths[0].path): CopyStream OOB Received: $($OOBCmd)";
                                break;
                            }
                        }
                    }
                }

                if(([DateTime]::Now - $lastRead).TotalMilliseconds -ge $shared.networkTimeout - 1000)
                {
                    #timed out
                    if($OOBCmd -ne 0)
                    {
                        #force return code to A (doesn't matter what the OOB was, we timed out)
                        Write-Error "Database Restore Error: $($source.paths[0].path) -> Timeout (changing received status from $($OOBCmd) to A)";
                        $OOBCmd = 'A';
                    }
                    #break out of the loop and return a failure (should be 10 or 11)
                    break;
                }
            }
        }

        return (OOBCopyStreamResult $OOBCmd);
    }
    catch
    {
        #if the other side disconnects the socket...
        #TODO: I don't believe we need this error, because it will randomly show that the socket was disconnected if the other side disconnects early
        #WriteErrorEx $_;

        return (OOBCopyStreamResult $OOBCmd);
    }
    finally
    {
        Write-Verbose "$($source.paths[0].path): Exiting CopyStream, OOB Received: $($OOBCmd), Transferred: $($readTotal.ToString("N0")) Bytes";

        if($shared.isDestination)
        {
            if($meta -ne $null)
            {
                #remove progress bar
                Write-Progress $progressActivity -Id $progressId -Completed;
            }
            $shared.metrics.BytesTransferred += $readTotal;
        }

        try
        {
            for($i = 0; $i -lt $writeStreams.Length; $i++)
            {
                try
                {
                    $writeStreams[$i].Flush();

                    #if the write stream is compressed, we need to close it to make sure all bytes write (close is safe to call twice)
                    if($writeStreams[$i].GetType() -eq [Zstandard.Net.ZstandardStream])
                    {
                        $writeStreams[$i].Close();
                    }
                }
                catch
                {
                    #ignore errors while flushing
                }

                if($writeStreams[$i].GetType() -eq [IO.FileStream])
                {
                    Write-Verbose "$($source.paths[0].path): Setting final file length to $($writeStreams[$i].Position)";
                    $writeStreams[$i].SetLength($writeStreams[$i].Position);
                }
            }
        }
        catch
        {
            WriteErrorEx $_;
        }
    }
}

function OOBCopyStreamResult
{
    [OutputType([int])]
    param
    (
        [Parameter(Mandatory)][char]$OOBCmd
    )

    if($OOBCmd -eq 0)
    {
        return 11; #fail, no OOB received
    }

    switch($OOBCmd)
    {
        'D'
        {
            return 0; #success, OOB received
            break;
        }

        'X'
        {
            return 2; #success, OOB X received (shut down afterwards)
            break;
        }

        'A'
        {
            return 10; #abort, OOB received
            break;
        }

        'P'
        {
            return 99; #skip, OOB received
            break;
        }

        'T'
        {
            return 12; #retry without RESTART option
            break;
        }

        'U'
        {
            return 13; #retry with CONTINUE_AFTER_ERROR option
            break;
        }

        default
        {
            Write-Error " ** INVALID OOB RECEIVED: $([byte]$OOBCmd) **";
            return 10; #abort, OOB received
            break;
        }
    }
}

#gets the default data/log paths for a given instance on the LOCAL SQL server (the paths will always end with a backslash)
function GetDefaultSQLPaths
{
    [OutputType([string],[string])]
    param
    (
        [Parameter()][string]$instance #the local instance, leave empty for default instance
    )

    try
    {
        #look up the default data/log paths
        if([string]::IsNullOrEmpty($instance)) { $instance = "MSSQLSERVER"; } #default instance

        [string]$instancePath = (Get-ItemProperty "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\SQL" $instance).$instance;

        [string]$dataPath = $null;
        [string]$logPath = $null;
        try
        {
            $dataPath = (Get-ItemProperty "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\$($instancePath)\MSSQLServer" DefaultData).DefaultData 2> $null;
        }
        catch { }
        try
        {
            $logPath = (Get-ItemProperty "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\$($instancePath)\MSSQLServer" DefaultLog).DefaultLog 2> $null;
        }
        catch { }

        if([string]::IsNullOrEmpty($dataPath)) { $dataPath = "C:\Program Files\Microsoft SQL Server\$($instancePath)\MSSQL\DATA\"; }
        if([string]::IsNullOrEmpty($logPath)) { $logPath = "C:\Program Files\Microsoft SQL Server\$($instancePath)\MSSQL\DATA\"; }
        if($dataPath[$dataPath.Length - 1] -ne "\") { $dataPath += "\"; }
        if($logPath[$logPath.Length - 1] -ne "\") { $logPath += "\"; }
        return $dataPath, $logPath;
    }
    catch
    {
        #WriteErrorEx $_;
        throw;
    }
}

#retrieve all database names from a wildcard, can be one, can be a lot
#if a wildcard expression is enclosed in { }, it is a SQL query that will be run to obtain the list of databases, one field must be returned with the alias "name"
#  i.e. {select name from sys.databases where name>'fred'}
function ExpandDatabases
{
    [OutputType([string[]])]
    param
    (
        [Parameter(Mandatory=$true)][string]$SQLServer,
        [Parameter(Mandatory=$true)][string]$dbWildcard,
        [Parameter()][int]$sinceHours,
        [Parameter()][bool]$randomizeOrder
    )

    try
    {
        if([string]::IsNullOrEmpty($dbWildcard)) { return [string[]]::new(0); }

        Write-Verbose "Expanding Databases: $($SQLServer)\$($dbWildcard)";

        $DB = [System.Data.SqlClient.SqlConnection]::new("Server=$($SQLServer);Database=master;Trusted_Connection=true");
        $DB.Open();

        [string]$SQLCmd = $null;
        if(![string]::IsNullOrEmpty($dbWildcard))
        {
            if($dbWildcard[0] -eq '{' -and $dbWildcard[$dbWildcard.Length-1] -eq '}')
            {
                $SQLCmd = $dbWildcard.Substring(1, $dbWildcard.Length - 2);
            }
            else
            {
                $dbWildcard = $dbWildcard.Replace("*", "%"); #turn any asterisks into SQL wildcard, * = %
                $dbWildcard = $dbWildcard.Replace("'", "''");
                $SQLCmd = "SELECT name FROM sys.sysdatabases WHERE name LIKE '$($dbWildcard)' AND name <> 'tempdb'";
            }
        }

        if($sinceHours -gt 0)
        {
            switch($shared.backupType)
            {
                "Full"
                {
                    $backupsetType = "'D'";
                    break;
                }
                "Differential"
                {
                    $backupsetType = "'D','I'";
                    break;
                }
                "Incremental"
                {
                    $backupsetType = "'D','L'";
                    break;
                }
            }
            $wherePos = $SQLCmd.IndexOf(" WHERE ", 0, [StringComparison]::OrdinalIgnoreCase);
            if($wherePos -ge 0)
            {
                $SQLCmd += " AND ";
            }
            else
            {
                $SQLCmd += " WHERE ";
            }
            $SQLCmd += " name NOT IN (SELECT database_name FROM msdb..backupset WHERE type IN ($($backupsetType)) GROUP BY database_name HAVING MAX(backup_start_date) >= DATEADD(hour,-$($sinceHours),'$($shared.startTime.ToString("s"))'))";
        }
        $DT = [System.Data.DataTable]::new();
        $DA = [System.Data.SqlClient.SqlDataAdapter]::new($SQLCmd, $DB);
        [void]$DA.Fill($DT);
        $DA.Dispose();

        $dbNames = [string[]]::new($DT.Rows.Count);
        for($i = 0; $i -lt $DT.Rows.Count; $i++)
        {
            $dbNames[$i] = $DT.Rows[$i]["name"];
        }
        $DT.Dispose();
        $DB.Close();

        if($randomizeOrder)
        {
            $r = [Random]::new();
            $dbNames = [string[]]($dbNames | Sort-Object {$r.Next()});
        }

        return $dbNames;
    }
    catch
    {
        #WriteErrorEx $_;
        throw;
    }
}

#retrieve all filenames for a given path that can contain wildcard characters, can be one, can be a lot
function ExpandFilenames
{
    [OutputType([string[]])]
    param
    (
        [Parameter(Mandatory=$true)][string]$pathWildcard,
        [Parameter()][bool]$randomizeOrder
    )

    try
    {
        $f = Get-ChildItem $pathWildcard -File;
        $fileNames = [string[]]::new($f.Count); #don't use $f.Length, it's the length of the file
        for($i = 0; $i -lt $f.Count; $i++)
        {
            $fileNames[$i] = $f[$i].FullName;
        }

        if($randomizeOrder)
        {
            $r = [Random]::new();
            $dbNames = [string[]]($dbNames | Sort-Object {$r.Next()});
        }

        return $fileNames;
    }
    catch
    {
        #WriteErrorEx $_;
        throw;
    }
}

#Connect to a SQL instance on the LOCAL system, Process SQL Command, and Stream to or From the VDI Depending on the Command (i.e. RESTORE/BACKUP)
#Wait for the task and then dispose of the VDI when complete
function StreamSQLCommand
{
    [OutputType([Threading.Tasks.Task[Exception]],[VdiDotNet.VdiEngine])]
    param
    (
        [Parameter(Mandatory=$true)][string]$SQLServer,
        [Parameter(Mandatory=$true)][string]$dbName,
        [Parameter()][string]$SQLPreCmd,
        [Parameter(Mandatory=$true)][string]$SQLCmd,
        [Parameter()][string]$SQLPostCmd,
        [Parameter(Mandatory=$true)][IO.Stream]$stream
    )

    try
    {
        Write-Verbose "$($SQLServer)\$($dbName): Creating and opening VDI";
        $vdi = [VdiDotNet.VdiEngine]::new($SQLServer, $dbName, $shared.networkTimeout - 1000);
        $SQLCmd = [string]::Format($SQLCmd, $vdi.VDIDeviceName);

        Write-Verbose "$($SQLServer)\$($dbName): SQL Executing: $SQLCmd";

        $vdi.DBExecute($SQLPreCmd, $SQLCmd, $SQLPostCmd);

        [Threading.Tasks.Task[Exception]]$task = $null;
        if($vdi.DBError -ne $null)
        {
            Write-Verbose "$($SQLServer)\$($dbName): Executing produced an Instant Error!";
            $task = [Threading.Tasks.Task[Exception]]::FromResult([Exception]$vdi.DBError);
        }
        else
        {
            $task = $vdi.RunStream($stream);
        }

        return $task,$vdi;
    }
    catch
    {
        #WriteErrorEx $_;
        throw;
    }
}

#receive regular file from stream, this runs on the destination
function ReceiveFile
{
    [OutputType([bool])]
    param
    (
        [Parameter(Mandatory=$true)][MetaData]$meta,
        [Parameter(Mandatory=$true)][Net.Sockets.TcpClient]$tcp,
        [Parameter(Mandatory=$true)][IO.Stream]$stream
    )

    [Zstandard.Net.ZstandardStream]$zstd = $null;
    [IO.FileStream]$fstream = $null;
    [string]$fileName = $null;
    [bool]$deleteFile = $false; #if we have a failure, do not leave an incomplete file
    try
    {
        $fileName = $meta.files[0].physicalPath;
        try
        {
            $fstream = [IO.FileStream]::new($fileName, [IO.FileMode]::Create, [IO.FileAccess]::Write, [IO.FileShare]::None, $shared.bufferSize, $true); #async
        }
        catch
        {
            #if the file is inaccessible, take ownership of the file and try again
            takeown.exe /f "$($fileName)"
            icacls.exe "$($fileName)" /reset

            $fstream = [IO.FileStream]::new($fileName, [IO.FileMode]::Create, [IO.FileAccess]::Write, [IO.FileShare]::None, $shared.bufferSize, $true); #async
        }
        #timeouts not supported in FileStream
        #$fstream.ReadTimeout = $shared.networkTimeout - 1000;
        #$fstream.WriteTimeout = $shared.networkTimeout - 1000;
        #$estimatedSize = EstimateFileSize $meta;
        #$stream.SetLength($estimatedSize);

        if($meta.compressionLevel -gt 0)
        {
            $zstd = [Zstandard.Net.ZstandardStream]::new($stream, [IO.Compression.CompressionMode]::Decompress, $true);
            $zstd.CompressionLevel = $meta.compressionLevel;
            $result = CopyStream $zstd $fstream $tcp -meta $meta;
        }
        else
        {
            #if there's already compression, just push the file as it is
            $result = CopyStream $stream $fstream $tcp -meta $meta;
        }

        switch($result)
        {
            { $_ -in 0,2 }
            {
                $null = SendOOBCommand $tcp 'D'; #one
                return $true;
                break;
            }

            1
            {
                $null = SendOOBCommand $tcp 'D'; #one
                #no OOB received, wait for it here... I don't think it matters what it is, we still succeeded to get all the bytes, should be D or X
                $OOBCmd = WaitOOBCommand $tcp;
                $result = OOBCopyStreamResult $OOBCmd;
                if($result -notin (0, 2))
                {
                    #test
                    Write-Error "Received Bad Result: $($OOBCmd) after successfully receiving all bytes from stream";
                }
                return $true;
                break;
            }

            10
            {
                $deleteFile = $true; #remove incomplete file
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "101`r`n"); } catch { }
                $null = SendOOBCommand $tcp 'A'; #bort
                return $false;
                break;
            }

            11
            {
                $deleteFile = $true; #remove incomplete file
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "102`r`n"); } catch { }
                $null = SendOOBCommand $tcp 'A'; #one
                #no OOB received, wait for it here... doesn't matter what it is, we still failed
                $OOBCmd = WaitOOBCommand $tcp;
                $result = OOBCopyStreamResult $OOBCmd;
                return $false;
                break;
            }

            12
            {
                $deleteFile = $true; #remove incomplete file
                $null = SendOOBCommand $tcp 'T'; #retry without RESTART option
                return $false;
                break;
            }

            13
            {
                $deleteFile = $true; #remove incomplete file
                $null = SendOOBCommand $tcp 'U'; #retry with CONTINUE_AFTER_ERROR option
                return $false;
                break;
            }

            99
            {
                $deleteFile = $true; #remove incomplete file
                $null = SendOOBCommand $tcp 'P'; #skiP
                return $false;
                break;
            }

            default
            {
                Write-Error "Unknown CopyStream Result: $result";
                return $false;
                break;
            }
        }
    }
    catch
    {
        $deleteFile = $true; #remove incomplete file
        WriteErrorEx $_;
    }
    finally
    {
        if($fstream -ne $null)
        {
            $fstream.Flush();
            $fstream.Close();
            $fstream.Dispose();
            $fstream = $null;
        }

        CloseObjects $null $null $null $null $zstd;

        if($deleteFile -and ![string]::IsNullOrEmpty($fileName)) { $null = Remove-Item $fileName 2> $null; } #remove incomplete file
    }
}

#process incoming SQL stream, save it to the -ToPath arguments (the local SQL instance, file, or both)
function ReceiveSQLStream
{
    param
    (
        [Parameter(Mandatory=$true)][MetaData]$meta,
        [Parameter(Mandatory=$true)][Net.Sockets.TcpClient]$tcp,
        [Parameter(Mandatory=$true)][IO.Stream]$stream,
        [Parameter()][datetime]$stopAt = [datetime]::MaxValue,
        [Parameter()][bool]$disableRestartOption = $false,
        [Parameter()][bool]$enableContinueAfterErrorOption = $false
    )

    [VdiDotNet.VdiEngine]$vdi = $null;
    [Zstandard.Net.ZstandardStream]$zstd = $null;
    $destStreams = [Collections.Generic.List[IO.Stream]]::new();
    [Exception]$DBError = $null;
    [Threading.Tasks.Task[Exception]]$destTask = $null;
    [string]$fileName = $null;
    [bool]$deleteFile = $false; #if we have a failure, do not leave an incomplete file
    $sqlServerInstances = [Collections.Generic.List[string]]::new();
    [int]$copyStreamResult = -1;

    #if we have compression on the incoming stream, and there is a flag to save files in the BAK format (which isn't using our compression),
    #then it is most efficient to decompress the incoming stream.
    #however, if we are saving files in our compressed DAT format, then we will want to apply decompression only on streams going to SQL, and any streams going to the filesystem will store the compressed data as is.
    [bool]$decompressedAtSource = $false;
    if($shared.saveFilesAsBak -and $meta.compressionLevel -gt 0)
    {
        #wrap the incoming stream in the decompression stream, so all streams going to both SQL and the filesystem will be uncompressed
        $zstd = [Zstandard.Net.ZstandardStream]::new($stream, [IO.Compression.CompressionMode]::Decompress, $true);
        $zstd.CompressionLevel = $meta.compressionLevel;
        $stream = $zstd;
        $decompressedAtSource = $true;
    }

    try
    {
        Write-Verbose "$($dest.server)\$($meta.name): Preparing for incoming SQL stream";

        for($i = 0; $i -lt $dest.paths.Length; $i++)
        {
            if($dest.paths[$i] -eq $null) { continue; }

            [IO.Stream]$destStream = $null;
            [DatabasePath]$p = $dest.paths[$i];
            switch($p.pathType)
            {
                0 #Restore to database
                {
                    if($meta.name -in "master","msdb","model")
                    {
                        Write-Verbose "$($dest.server)\$($meta.name): Skipping SQL restore of system database.";
                        continue;
                    }

                    Write-Verbose "$($dest.server)\$($meta.name): Preparing database stream";
                    $destStream = [EchoStream]::new(100);
                    $destStream.ReadTimeout = $shared.networkTimeout - 1000; #s seconds less just in case we get a timeout, we want to make sure we drop off first
                    $destStream.WriteTimeout = $shared.networkTimeout - 1000;
                    $destStream.CopyBufferOnWrite = $true; #you'll corrupt memory if this isn't set to true

                    #decompress stream going to SQL if the compression level is greater than 0 and the stream isn't already decompressed
                    if(!$decompressedAtSource -and $meta.compressionLevel -gt 0)
                    {
                        $zstd = [Zstandard.Net.ZstandardStream]::new($destStream, [IO.Compression.CompressionMode]::Decompress, $true);
                        $zstd.CompressionLevel = $meta.compressionLevel;

                        #hook up restore to stream
                        $destTask,$vdi = RestoreDatabase $p.path $meta $zstd $stopAt $disableRestartOption $enableContinueAfterErrorOption $p.namesWildcard;
                    }
                    else
                    {
                        #hook up restore to stream
                        $destTask,$vdi = RestoreDatabase $p.path $meta $destStream $stopAt $disableRestartOption $enableContinueAfterErrorOption $p.namesWildcard;
                    }
                    if(!$sqlServerInstances.Contains($p.path)) { $sqlServerInstances.Add($p.path); }

                    break;
                }

                1 #Copy to file
                {
                    if($meta.backupType -eq 0)
                    {
                        #if we are receiving a full backup, check to see if we requested a full backup. If not, then we need to figure out if it is an initial seed or a system database (both always run as full backups)
                        #if it is an initial seed, and the flag SkipFilePathOnInitialSeeding is set, then we need to skip the file streaming.
                        #do not skip system files, they are not seeding, they just always backup as full
                        if($shared.backupType -ne "Full" -and $shared.skipFilePathOnInitialSeeding -and $meta.name -notin "master","msdb","model")
                        {
                            continue;
                        }
                    }

                    $extension = if($shared.saveFilesAsBak) { "bak" } else { "dat" };
                    $fileName = "$($p.path)$($meta.name).$($extension)";
                    Write-Verbose "$($dest.server)\$($meta.name): Preparing file stream: $fileName";
                    #$mode = [IO.FileMode]::CreateNew;

                    try
                    {
                        if([IO.File]::Exists($fileName))
                        {
                            if($shared.replace)
                            {
                                #delete the file because we're going to replace it
                                [System.IO.File]::Delete($fileName);
                            }
                            else
                            {
                                throw "Skip: File $($fileName) exists, use -Replace parameter if you want to overwrite.";
                            }
                        }
                        #underscore added to filename to detonate a temporary file, it will be renamed when it finishes
                        $destStream = [IO.FileStream]::new("$($fileName)_", [IO.FileMode]::Create, [IO.FileAccess]::Write, [IO.FileShare]::None, $shared.bufferSize, $true); #async
                        #timeouts not supported in FileStream
                        #$destStream.ReadTimeout = $shared.networkTimeout - 1000;
                        #$destStream.WriteTimeout = $shared.networkTimeout - 1000;
                        #TODO: make sure this is set in the CopyStream
                        #$estimatedSize = EstimateFileSize $meta;
                        #$destStream.SetLength($estimatedSize);
                    }
                    catch
                    {
                        $fileName = $null; #this will prevent the file from being deleted/renamed below, since we weren't able to create it in the first place
                        throw;
                    }

                    #if we are saving files in our compressed DAT format, write the magic bytes and metadata
                    if(!$shared.saveFilesAsBAK)
                    {
                        #write magic bytes
                        $b = [Text.Encoding]::GetEncoding(1252).GetBytes("MST");
                        $destStream.Write($b, 0, $b.Length);

                        #write metadata
                        PutMetaData $meta $destStream;
                    }

                    break;
                }
            }
            #add stream to list of destination streams to write to
            if($destStream -ne $null) { $destStreams.Add($destStream); }
        }

        if($destStreams.Count -eq 0)
        {
            $DBError = [Exception]::new("Skip: Skipping Database");
        }
        else
        {
            #cross the streams, this will copy the source stream to all the destination streams, allows writing to both SQL and the filesystem simultaneously
            Write-Verbose "$($dest.server)\$($meta.name): Receiving stream...";

            $copyStreamResult = CopyStream $stream $destStreams.ToArray() $tcp $destTask $meta $vdi;
            Write-Verbose "$($dest.server)\$($meta.name): CopyStream result=$($copyStreamResult)";

            if($copyStreamResult -ge 10 -and $copyStreamResult -ne 11) #10, 12, 13 or above is a fail/restart received from the source (11 is a fail from the destination, where the source hasn't sent an OOB message yet)
            {
                #something went wrong at the source
                $DBError = [Exception]::new("Internal: Backup Source Has Sent An Abort, CopyStream Result=$($copyStreamResult)");

                if($vdi -ne $null)
                {
                    #SQL VDI abort if something went wrong
                    $vdi.AbortStream();
                    #make sure task is stopped, ignoring any exceptions it throws
                    $tasks = [Threading.Tasks.Task[]]::new(1);
                    $tasks[0] = $destTask;
                    if([System.Threading.Tasks.Task]::WaitAny($tasks, $shared.networkTimeout) -eq -1) { Write-Error "Database Restore Error: $($dest.server)\$($meta.name) -> Cannot Abort VDI Task!?"; }
                }
                $deleteFile = $true; #remove incomplete file
            }
            else
            {
                #make sure we wait for the restore task to finish (should be already) before we close any streams
                if($destTask -ne $null)
                {
                    if($destTask.Wait([Threading.Timeout]::Infinite)) #wait indefiniately for the restore to finish, use $destTask.Wait( ) to check for exceptions thrown
                    {
                        #Restore task finished, check for errors in the finally below
                    }
                    else #DEAD CODE: destTask.Wait(INFINITE) will either return true, or throw an exception, but never return false
                    {
                        Write-Error "$($dest.server)\$($meta.name): Restore task not done, calling abort";
                        $deleteFile = $true; #remove incomplete file
                        $vdi.AbortStream();
                        #make sure task is stopped, ignoring any exceptions it throws
                        $tasks = [Threading.Tasks.Task[]]::new(1);
                        $tasks[0] = $destTask;
                        if([System.Threading.Tasks.Task]::WaitAny($tasks, 5000) -gt -1)
                        {
                            #Restore task finished, check for errors in the finally block below
                        }
                        else
                        {
                            Write-Error "Database Restore Error: $($dest.server)\$($meta.name) -> Cannot Abort VDI Task!?";
                        }
                    }
                }
            }
        }
    }
    catch
    {
        #if we don't have a copystream result, set it to "failed with no OOB received"
        if($copyStreamResult -eq -1) { $copyStreamResult = 11; }

        #abort the VDI stream first, in case the psremote session is exiting...
        #else we can get a stuck job if you write to the error stream first, because that can cause you to throw an error up to the next function
        if($vdi -ne $null)
        {
            $vdi.AbortStream();
            #make sure the VDI has finished before we move onto the finally
            $tasks = [Threading.Tasks.Task[]]::new(1);
            $tasks[0] = $destTask;
            if([System.Threading.Tasks.Task]::WaitAny($tasks, [Threading.Timeout]::Infinite) -eq -1) { Write-Error "Database Restore Error: $($dest.server)\$($meta.name) -> Cannot Abort VDI Task!?"; }
        }

        if($_.Exception.Message.Length -gt 5 -and $_.Exception.Message.SubString(0, 5) -eq "Skip:")
        {
            #purposeful skip
            $DBError = [Exception]::new($_.Exception.Message);
        }
        else
        {
            $DBError = [Exception]::new("Internal: $($_.Exception.Message)");
            WriteErrorEx $_ $destTask "$($dest.server)\$($meta.name): $($_.Exception.Message.SubString(0,9))";
        }
        #throw;
    }
    finally
    {
        if($vdi -ne $null -and $DBError -eq $null)
        {
            $DBError = $vdi.DBError; #technically we could also use the task.result?
        }

        #check for any database errors to check the restore result, and send an OOB status command back to the source
        #we will default to an abort message, unless it is set to something else
        [char]$OOBCmd = 'A';

        if($shared.autoRestoreToDateTime -gt [datetime]::MinValue)
        {
            if($copyStreamResult -in (0, 2))
            {
                #if the restore is successful, send a 'D'one message
                $OOBCmd = 'D';
            }

            $deleteFile = $false; #there isn't ever an incomplete file when doing an autoRestore, so this can be false

            switch(CheckDBError $DBError) #do not break on any of these case statements, because there may be more than one returned!
            {
                -1 #log error
                {
                    Write-Error "Database Restore Error: $($source.paths[0].path)\$($meta.name) -> $($DBError.Message)";
                }
                #if we are running a restore, then don't worry about any other errors...
            }
        }
        else
        {
            switch(CheckDBError $DBError) #do not break on any of these case statements, because there may be more than one returned!
            {
                -2 #skip
                {
                    #used if we are skipping a database/file (i.e. a backup file exists but we don't use -replace to overwite)
                    $OOBCmd = 'P'; #skiP command
                }

                -1 #log error
                {
                    Write-Error "Database Restore Error: $($source.paths[0].path)\$($meta.name) -> $($DBError.Message)";
                }

                0 #all good
                {
                    Write-Verbose "$($dest.server)\$($meta.name): Sending notification: Success";
                    if(CheckForStopProcessing)
                    {
                        $OOBCmd = 'X';
                    }
                    else
                    {
                        $OOBCmd = 'D';
                    }
                }

                1 #an error has occurred, make sure we delete any incomplete file (if one exists). This will send back an 'A'bort by default
                {
                    $deleteFile = $true; #remove incomplete file
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "103`r`n"); } catch { }
                    if(-1 -notin (CheckDBError $DBError))
                    {
                        Write-Error "Database Restore Error: !MUTED ERROR! $($source.paths[0].path)\$($meta.name) -> $($DBError.Message)";
                    }
                }

                11 #retry with reseed (full backup)
                {
                    if($meta.backupType -ne 0 -and !$shared.copyOnly -and $shared.initialSeed -ne "NoSeed")
                    {
                        Write-Verbose "$($dest.server)\$($meta.name): Sending notification: Seed full backup due to error -> $($DBError.Message)";
                        $OOBCmd = 'F';
                    }
                    else
                    {
                        #we're already running a full backup, so ... abort?!?
                        #Write-Verbose "$($dest.server)\$($meta.name): Sending notification: Abort";
                        Write-Error "Database Restore Error: $($dest.server)\$($meta.name) -> ERROR WHEN ALREADY RUNNING FULL BACKUP: $($DBError.Message)";
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "104`r`n"); } catch { }
                        $OOBCmd = 'A';
                    }
                    $deleteFile = $true; #remove incomplete file
                }

                12 #retry without RESTART option
                {
                    Write-Verbose "$($dest.server)\$($meta.name): Sending notification: Retry, we will disable the OPTION RESTART due to error -> $($DBError.Message)";
                    $OOBCmd = 'T';
                    $deleteFile = $true; #remove incomplete file
                }

                13 #retry with CONTINUE_AFTER_ERROR option
                {
                    Write-Verbose "$($dest.server)\$($meta.name): Sending notification: Retry, we will enable OPTION CONTINUE_AFTER_ERROR due to error -> $($DBError.Message)";
                    $OOBCmd = 'U';
                    $deleteFile = $true; #remove incomplete file
                }

                14 #restart sql server and retry same database (snapshot volume probably went offline)
                {
                    #clear any VSS protection faults, and if we went offline, also restart SQL services
                    #[bool]$wasCreated = $false;
                    #$mutex = [Threading.Mutex]::new($false, "Global\PowerBakVSSFaulted", [ref]$wasCreated);
                    #try
                    #{
                    #    if(!$wasCreated)
                    #    {
                    #        #wait for volume to clear/SQL to restart
                    #        $null = $mutex.WaitOne();
                    #    }
                    #    else
                    #    {
                    #        $volume = ([regex]::Match($DBError.Message, "[A-Z]:\\")).Value
                    #        if(![string]::IsNullOrEmpty($volume))
                    #        {
                    #            #we are the first thread through, so we must clear the volume and restart SQL... all others will wait
                    #            Write-Information "Attempting to clear VSS fault on volume $($volume)";
                    #
                    #            #bring VSS back online if it was offline
                    #            $snapStatus = ClearVolumeProtectFault $volume;
                    #
                    #            #if it was offline, also restart the SQL servers
                    #            if($snapStatus.VolumeIsOfflineForProtection -eq $true)
                    #            {
                    #                Write-Information "Attempting to restart SQL services";
                    #                #gather SQL instances that need to restart
                    #                $null = Get-Service; #need this so [ServiceProcess.ServiceController] type will exist
                    #                $services = [Collections.Generic.List[ServiceProcess.ServiceController]]::new();
                    #                foreach($sqlServerInstance in $sqlServerInstances)
                    #                {
                    #                    $instance = ($sqlServerInstance -split "\\")[1];
                    #                    [string]$serviceName = $null;
                    #                    if([string]::IsNullOrEmpty($instance))
                    #                    {
                    #                        $serviceName = "MSSQLSERVER";
                    #                    }
                    #                    else
                    #                    {
                    #                        $serviceName = "MSSQL`$$($instance)";
                    #                    }
                    #                    $services.Add((Get-Service $serviceName));
                    #                }
                    #                foreach($service in $services) { try { $service.Stop(); } catch { } }
                    #                $services.WaitForStatus([System.ServiceProcess.ServiceControllerStatus]::Stopped, [timespan]::new(0, 5, 0));
                    #                foreach($service in $services) { try { $service.Start(); } catch { } }
                    #                $services.WaitForStatus([System.ServiceProcess.ServiceControllerStatus]::Running, [timespan]::new(0, 5, 0));
                    #            }
                    #        }
                    #    }
                    #}
                    #catch
                    #{
                    #    WriteErrorEx $_;
                    #}
                    #finally
                    #{
                    #    if($mutex -ne $null)
                    #    {
                    #        $mutex.ReleaseMutex(); #release next waiting thread
                    #        $mutex.Close();
                    #        $mutex.Dispose();
                    #        $mutex = $null;
                    #    }
                    #}
                    Start-Sleep -Milliseconds 60000;

                    $OOBCmd = if($disableRestartOption) { 'T' } else { if($enableContinueAfterErrorOption) { 'U' } else { 'S' } }; #request retry of same database
                    $deleteFile = $true; #remote incomplete file
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "meltdown`r`n"); } catch { }
                }
            }
        }

        $null = SendOOBCommand $tcp $OOBCmd;
        if($copyStreamResult -in (1,11))
        {
            #I don't think we need this return value... this is just so the other side gets a value through
            $OOBCmd = WaitOOBCommand $tcp;
        }

        Write-Verbose "$($dest.server)\$($meta.name): Closing streams";
        for($i = 0; $i -lt $destStreams.Count; $i++)
        {
            $destStreams[$i].Flush();
            $destStreams[$i].Close();
            $destStreams[$i].Dispose();
            $destStreams[$i] = $null;
        }

        CloseObjects $null $null $vdi $destTask $zstd;
        $destTask = $null;
        $zstd = $null;

        if(![string]::IsNullOrEmpty($fileName))
        {
            if($deleteFile)
            {
                #remove incomplete tmp file
                Write-Verbose "$($fileName): Deleting incomplete file";
                $null = Remove-Item "$($fileName)_" 2> $null;
            }
            else
            {
                #rename tmp file from filename.xxx_ -> filename.xxx when completed successfully
                Write-Verbose "$($fileName): Renaming file without stub";
                Rename-Item "$($fileName)_" $fileName;
            }
        }
    }
}

#connect to destination server and send a single file
#to stream a SQL DAT file, omit the $fileCopy switch
#to simply copy a file (any file/extension), set the $fileCopy switch, and also include the full remote path in the $remoteName parameter
function SendFile
{
    param
    (
        [Parameter(Mandatory=$true)][string]$filename,
        [Parameter()][string]$remoteName, #if this is a regular file copy, this must be the remote path/name, if this is a SQL restore it can be empty to use the name in the file, or you can override the destination database name
        [Parameter()][switch]$fileCopy, #just copy a file, if not set will be a SQL stream (fileCopy can also be used expand our DAT file into a regular BAK file)
        [Parameter()][datetime]$stopAt = [datetime]::MaxValue #when sending a SQL stream, if it is a log file, determine where to stop at
    )

    [Zstandard.Net.ZstandardStream]$zstd = $null;
    [IO.FileStream]$fstream = $null;
    [Net.Sockets.TcpClient]$tcp = $null;
    [IO.Stream]$networkStream = $null;
    try
    {
        [MetaData]$meta = $null;
        [string]$command = $null;

        if($fileCopy -and [string]::IsNullOrEmpty($remoteName)) { throw "Must provide a remote Path/Name when copying a regular file."; }

        $fstream = [IO.FileStream]::new("$($filename)", [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::None, $shared.bufferSize, $true); #async
        #timeouts not supported in FileStream
        #$fstream.ReadTimeout = $shared.networkTimeout - 1000;
        #$fstream.WriteTimeout = $shared.networkTimeout - 1000;

        $b = [byte[]]::new(3);
        $i = $fstream.Read($b, 0, $b.Length);
        if($i -eq $b.Length -and [Text.Encoding]::GetEncoding(1252).GetString($b) -eq "MST")
        {
            #this is our DAT file format
            #get metadata of database to be sent
            $meta = GetMetaData $fstream;
            if($meta -eq $null) { throw "Could not read Metadata: $($filename)"; }
        }
        else
        {
#TODO: maybe find a way to obtain the logical filenames for a SQL restore from a bak file?
            if(!$fileCopy) { throw "Currently only SQL restores can be done with the DAT format."; }

            #rewind the 3 bytes we read above
            $null = $fstream.Seek(0L, [IO.SeekOrigin]::Begin);

            #create faux metadata of file to be sent
            $meta = [MetaData]::new();
            $meta.compressionLevel = 0;
            $meta.name = [IO.Path]::GetFileName($remoteName);
        }

        if($fileCopy)
        {
            $command = 'F';
            $meta.backupType = -1;
            $meta.files = [MetaFile[]]::new(1);
            $meta.files[0] = [MetaFile]::new();
            [MetaFile]$file = $meta.files[0];
            $file.type = -1;
            $file.size = $fstream.Length;
            $file.used = $file.size;
            $file.physicalPath = $remoteName; #where we want the file saved
        }
        else
        {
            #SQL Restore
            $command = 'S';
            if($stopAt -lt [datetime]::MaxValue)
            {
                #append stopat date to command
                $command += $stopAt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss");
            }

            if(![string]::IsNullOrEmpty($remoteName))
            {
                #override remote database name during restore
                $meta.name = $remoteName;
            }

            #let's first see if we need to perform a DBCC CHECKDB on the destination, but if we are sending a differential/incremental backup type
            #i.e. if we're sending a full backup, there is no need to run a DBCC CHECKDB because the database is getting overwritten anyways
            if($shared.toCheckDB -and $meta.backupType -in 1,2)
            {
                try
                {
                    $checkDBResult = ExecuteDBCommand "DBCC CHECKDB([$($meta.name)])";
                    if($checkDBResult -ne "0")
                    {
                        #TODO: what to do with a checkdb error?  Reseed?
                        #      what if the same error exists on the source?
                        Write-Information "DBCC CHECKDB FAILED: $($checkDBResult)";
                    }
                    else
                    {
                        Write-Information "DBCC SUCCEEDED: $($checkDBResult)";
                    }
                }
                catch
                {
                    throw "DBCC CheckDB Failed";
                }
            }
        }

        #if the file isn't already compressed, but we are requesting compression, then compress it as it sends
        $fileCompression = $meta.compressionLevel;
        if($fileCompression -eq 0 -and $shared.compressionLevel -gt 0)
        {
            $meta.compressionLevel = $shared.compressionLevel;
#Not compressing when sending mdf/ldf files??
#Write-Verbose "::SendFile: Adding Compression Level=$($meta.compressionLevel)";
        }

        [bool]$copyComplete = $false;
        $startingFilePosition = $fstream.Position;
        while(!$copyComplete)
        {
            CloseObjects $tcp $networkStream $null $null $zstd;
            $tcp = $null;
            $networkStream = $null;
            $zstd = $null;

            #connect and transfer the file
            $abort = $false;
            $tcp,$networkStream = ConnectToDestinationServer $command $meta ([ref]$abort);
            if($abort)
            {
                $null = SendOOBCommand $tcp 'P'; #signal skip
                $shared.metrics.Skipped++; #do not increment fail counter, because technically nothing failed
                return;
            }

            #add compression to the stream if there wasn't any compression in the first place
            [int]$result = $false;
            if($fileCompression -eq 0 -and $meta.compressionLevel -gt 0)
            {
                $zstd = [Zstandard.Net.ZstandardStream]::new($networkStream, [IO.Compression.CompressionMode]::Compress, $true);
                $zstd.CompressionLevel = $meta.compressionLevel;
                $result = CopyStream $fstream $zstd $tcp -meta $meta;
            }
            else
            {
                #if there's already compression, just push the file as it is
                $result = CopyStream $fstream $networkStream $tcp -meta $meta;
            }

            $copyComplete = $true;
#test
Write-Verbose "::SendFile: CopyStream, results=$($result)";
            switch($result)
            {
                { $_ -in 0, 2 }
                {
                    #we received a success OOB and stopped transmitting
                    Write-Verbose "$($source.paths[0].path): Received Status Message From Destination: Success";
                    $null = SendOOBCommand $tcp 'D'; #signal far end that we succeeded
                    $shared.metrics.Success++;
                    break;
                }

                1
                {
                    #we finished streaming, but have not received an OOB message from the far end
                    #signal far end that we succeeded
                    $null = SendOOBCommand $tcp 'D';

                    #wait for far end to send back a success message
                    $OOBCmd = WaitOOBCommand $tcp;

                    Write-Verbose "::SendFile: Received OOB=$($OOBCmd)";
                    if((OOBCopyStreamResult $OOBCmd) -in (0, 2))
                    {
                        $shared.metrics.Success++;
                    }
                    else
                    {
                        $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "1`r`n"); } catch { }
                    }
                    break;
                }

                10
                {
                    #we received a fail OOB and stopped transmitting
                    Write-Verbose "$($source.paths[0].path): Received Status Message From Destination: Abort";
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "105`r`n"); } catch { }
                    $null = SendOOBCommand $tcp 'A'; #signal far end that we aborted
                    $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "2`r`n"); } catch { }
                    break;
                }

                11
                {
                    #we failed, and have not received a message from the far end
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "106`r`n"); } catch { }
                    $null = SendOOBCommand $tcp 'A'; #signal far end that we aborted
                    $null = WaitOOBCommand $tcp; #receive back whatever the far end sends, doesn't matter, we already failed
                    $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "3`r`n"); } catch { }
                    break;
                }

                12
                {
                    #retry without RESTART option
                    if($command -eq 'S')
                    {
                        #retry backup
                        $null = SendOOBCommand $tcp 'T';
                        $command = 'T';
                        Write-Information "$($filename): Retrying restore without OPTION RESTART";
                        $shared.metrics.Retry++;

                        #do NOT break out of the file copy loop because we are sending the backup again
                        $copyComplete = $false;

                        #reset source file position
                        $fstream.Position = $startingFilePosition;
                    }
                    elseif($command -eq 'T')
                    {
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "107`r`n"); } catch { }
                        $null = SendOOBCommand $tcp 'A'; #abort

                        $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "4`r`n"); } catch { }
                        Write-Error "Database Restore Error: $($filename) -> Requested to retry backup without OPTION RESTART, but we already did!?";
                    }
                    else
                    {
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "108`r`n"); } catch { }
                        $null = SendOOBCommand $tcp 'A'; #abort

                        $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "4b`r`n"); } catch { }
                        Write-Error "Database Restore Error: $($filename) -> Cannot request to retry without RESTART option for file copy!? (this should never happen)";
                    }
                    break;
                }

                13
                {
                    #retry with CONTINUE_AFTER_ERROR option
                    if($command -eq 'S')
                    {
                        #retry backup
                        $null = SendOOBCommand $tcp 'U';
                        $command = 'U';
                        Write-Information "$($filename): Retrying restore with OPTION CONTINUE_AFTER_ERROR";
                        $shared.metrics.Retry++;

                        #do NOT break out of the file copy loop because we are sending the backup again
                        $copyComplete = $false;

                        #reset source file position
                        $fstream.Position = $startingFilePosition;
                    }
                    elseif($command -eq 'U')
                    {
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "109`r`n"); } catch { }
                        $null = SendOOBCommand $tcp 'A'; #abort

                        $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "5`r`n"); } catch { }
                        Write-Error "Database Restore Error: $($filename) -> Requested to retry backup with OPTION CONTINUE_AFTER_ERROR, but we already did!?";
                    }
                    else
                    {
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "110`r`n"); } catch { }
                        $null = SendOOBCommand $tcp 'A'; #abort

                        $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "6`r`n"); } catch { }
                        Write-Error "Database Restore Error: $($filename) -> Cannot request to retry with CONTINUE_AFTER_ERROR option for file copy!? (this should never happen)";
                    }
                    break;
                }

                99
                {
                    #we received a skip OOB and stopped transmitting
                    Write-Verbose "$($source.paths[0].path): Received Status Message From Destination: Abort";
                    $null = SendOOBCommand $tcp 'P'; #signal far end that we skipped
                    $shared.metrics.Skipped++;
                    break;
                }
            }
        }
    }
    catch
    {
        WriteErrorEx $_;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "111`r`n"); } catch { }
        $null = SendOOBCommand $tcp 'A'; #signal early abortion
        $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "7`r`n"); } catch { }
        #throw;
    }
    finally
    {
        Write-Verbose "$($source.paths[0].path): Exiting SendFile";

        if($fstream -ne $null)
        {
            $fstream.Close();
            $fstream.Dispose();
            $fstream = $null;
        }

        CloseObjects $tcp $networkStream $null $null $zstd;
        $tcp = $null;
        $networkStream = $null;
        $zstd = $null;
    }
}

#connect to destination and restore a single database to a specific point in time
function AutoRestoreDatabase
{
    param
    (
       [Parameter(Mandatory=$true)][string]$BackupPath,
       [Parameter(Mandatory=$true)][string]$namesWildcard,
       [Parameter(Mandatory=$true)][DateTime]$ToDateTime,
       [Parameter()][string]$newDbName #if only restoring a single database (not a wildcard), this can override the name of the new database
    )

    try
    {
        #get a list of matching MDF files, these are the databases that we need to restore
        $dbList = Get-ChildItem $BackupPath "$($namesWildcard).mdf" -Recurse -File;

        #as there can be multiple snapshots, we can have the same file duplicated, so we need to reduce this down to a unique list
        $dbListUnique = $dbList.Name | Get-Unique;

        #sanity validation
        if(![string]::IsNullOrEmpty($newDBName) -and $dbListUnique.Count -gt 1)
        {
            throw "Cannot restore multiple databases [$($dbListUnique.Count)] when destination database name is specified";
        }

        [IO.FileInfo]$dbItem = $null;
        foreach($dbItem in $dbListUnique)
        {
            $shared.metrics.AutoRestore++;

            $dbName = [IO.Path]::GetFileNameWithoutExtension($dbItem.Name);
            if([string]::IsNullOrEmpty($newDBName)) { $newDBName = $dbName; }

            #get a list of matching MDF/LDF/DAT files
            #then sort the files by date descending
            $files = Get-ChildItem $BackupPath "$($dbName)*" -Recurse -File | Where-Object { $_.Name -match "$($dbName)_*(?:log)?\.(?:mdf|ldf|und|dat|bak)" } | Sort-Object LastWriteTime -Descending;

            #keep track of the matching mdf/ldf/und files for restoring
            [IO.FileSystemInfo]$fileMDF = $null;
            [IO.FileSystemInfo]$fileLDF = $null;
            [IO.FileSystemInfo]$fileUND = $null;
            $fileDeltas = [Collections.Generic.List[IO.FileSystemInfo]]::new();

            #also keep track of the earliest mdf/ldf/und, in case we don't find any in the requested data range, we can at least offer the earliest date
            [IO.FileSystemInfo]$earliestFileMDF = $null;
            [IO.FileSystemInfo]$earliestFileLDF = $null;
            [IO.FileSystemInfo]$earliestFileUND = $null;

            #loop through the files gathering the ones we need for the restore
            [IO.FileSystemInfo]$file = $null;
            foreach($file in $files)
            {
                switch($file.Extension)
                {
                    ".mdf"
                    {
                        $earliestFileMDF = $file;
                        if($file.LastWriteTime -gt $ToDateTime) { continue; } #skip MDF newer than requested restore date
                        Write-Verbose "$($source.paths[0].path): Found MDF: $($file.Name) - $($file.LastWriteTime.ToString())";
                        if($fileMDF -eq $null) { $fileMDF = $file; }
                        break;
                    }

                    ".ldf"
                    {
                        $earliestFileLDF = $file;
                        if($file.LastWriteTime -gt $ToDateTime) { continue; } #skip LDF newer than requested restore date
                        Write-Verbose "$($source.paths[0].path): Found LDF: $($file.Name) - $($file.LastWriteTime.ToString())";
                        if($fileLDF -eq $null) { $fileLDF = $file; }
                        break;
                    }

                    ".und"
                    {
                        $earliestFileUND = $file;
                        if($file.LastWriteTime -gt $ToDateTime) { continue; } #skip UND newer than requested restore date
                        Write-Verbose "$($source.paths[0].path): Found UNDO: $($file.Name) - $($file.LastWriteTime.ToString())";
                        if($fileUND -eq $null) { $fileUND = $file; }
                        break;
                    }

                    { $_ -in ".dat",".bak" }
                    {
                        #add all DAT files until we find the MDF/LDF
                        if($fileMDF -eq $null -or $fileLDF -eq $null)
                        {
                            Write-Verbose "$($source.paths[0].path): Found delta: $($file.Name) - $($file.LastWriteTime.ToString())";
                            $fileDeltas.Add($file);
                        }
                        break;
                    }

                    default
                    {
                        #unknown file extension
                        Write-Verbose "$($source.paths[0].path): Unknown File Extension: $($file.FullName)";
                        break;
                    }
                }
            }

            if($fileMDF -eq $null -or $fileLDF -eq $null)
            {
                if($earliestFileMDF -eq $null -or $earliestFileLDF -eq $null)
                {
                    Write-Error "Database Restore Error: Cannot find MDF or LDF for $($p.namesWildcard) in $($p.path)";
                    $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "8`r`n"); } catch { }
                    continue; #skip to next database to restore
                }
                else
                {
                    $confirmation = Read-Host "Nearest MDF/LDF for $($p.namesWildcard) in $($p.path) has a date of $($earliestFileMDF.LastWriteTime), would you like to restore this? (y/n)";
                    if($confirmation -ne "y")
                    {
                        $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "9`r`n"); } catch { }
                        continue; #skip to next database to restore
                    }
                    else
                    {
                        #restore the mdf/ldf with the closest time, even though it is earlier than the date requested
                        #clear out any transaction logs and set up the mdf/ldf/und
                        $fileMDF = $earliestFileMDF;
                        $fileLDF = $earliestFileLDF;
                        $fileUND = $earliestFileUND;
                        $fileDeltas.Clear();
                    }
                }
            }

            [Net.Sockets.TcpClient]$tcp = $null;
            [IO.Stream]$networkStream = $null;
            try
            {
                $meta = [MetaData]::new();
                $meta.name = $newDbName;
                $abort = $false;
                $tcp,$networkStream = ConnectToDestinationServer 'R' $meta ([ref]$abort); #start auto restore
                if($abort)
                {
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "112`r`n"); } catch { }
                    $null = SendOOBCommand $tcp 'A'; #signal early abortion
                    return;
                }

                [char]$OOBCmd = 0;

                #wait for auto-restore to begin
                $startTime = [datetime]::Now;
                while(([datetime]::Now - $startTime).TotalMilliseconds -lt $shared.networkTimeout)
                {
                    $OOBCmd = GetOOBCommand $tcp;
                    if($OOBCmd -ne 0) { break; }
                    Start-Sleep -Milliseconds 100;
                }

                if($OOBCmd -ne 'D' -and $OOBCmd -ne 'X')
                {
                    $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "10`r`n"); } catch { }
                    throw "Could not begin auto restore";
                }

                $meta = GetMetaData $networkStream;
                if($meta -eq $null) { throw "Could not get meta data back after beginning auto restore"; }

                #close socket so destination will listen for another connection
                CloseObjects $tcp $networkStream;
                $tcp = $null;
                $networkStream = $null;

                [MetaFile]$destMDF = $null;
                [MetaFile]$destLDF = $null;
                [MetaFile]$destUND = $null;
                [MetaFile]$metafile = $null;
                foreach($metafile in $meta.files)
                {
                    switch($metafile.type)
                    {
                        0 #MDF
                        {
                            $destMDF = $metafile;
                            Write-Verbose "$($source.paths[0].path): Sending File: $($fileMDF.FullName) -> $($destMDF.physicalPath)";
                            SendFile $fileMDF.FullName $destMDF.physicalPath -fileCopy
                            break;
                        }

                        1 #LDF
                        {
                            $destLDF = $metafile;
                            Write-Verbose "$($source.paths[0].path): Sending File: $($fileLDF.FullName) -> $($destLDF.physicalPath)";
                            SendFile $fileLDF.FullName $destLDF.physicalPath -fileCopy;
                            break;
                        }

                        9 #UND (we made up this number, it's not actually type 9)
                        {
                            if($fileUND -ne $null)
                            {
                                $destUND = $metafile;
                                Write-Verbose "$($source.paths[0].path): Sending File: $($fileUND.FullName) -> $($destUND.physicalPath)";
                                SendFile $fileUND.FullName $destUND.physicalPath -fileCopy;
                            }
                            break;
                        }
                    }
                }

                if($fileUND -ne $null -and $destLDF -ne $null)
                {
                    #now that we have copied in the MDF/LDF files, binary search/replace the old UND path/filename in the LDF to the newly created UND path/filename
                    Write-Verbose "$($source.paths[0].path): Repointing undo path in LDF"
                    $metaRegEx = [MetaData]::new();
                    $metaRegEx.files = [MetaFile[]]::new(3);
                    $metaRegEx.files[0] = $destLDF; #file to search/replace
                    $srcUND = [MetaFile]::new();
                    $srcUND.physicalPath = "[C-Z][\x00]:[\x00]\\[\x00](?:[\x20-\x7f][\x00])+" + [Text.RegularExpressions.RegEx]::Replace($fileUND.Name, '[^\\]|\\(?!\\)', '$0[\x00]', [Text.RegularExpressions.RegexOptions]::SingleLine);
                    $destUND.physicalPath = ([Text.RegularExpressions.RegEx]::Replace($destUND.physicalPath, '[^`]', "`$0`0", [Text.RegularExpressions.RegexOptions]::SingleLine) + "`0`0");
                    $metaRegEx.files[1] = $srcUND; #search string
                    $metaRegEx.files[2] = $destUND; #replace string

                    #have destination server run and binary search/replace and wait for it to finish
                    $tcp,$networkStream = ConnectToDestinationServer 'B' $metaRegEx; #binary replace UND path in the LDF
                    $startTime = [datetime]::Now;
                    while(([datetime]::Now - $startTime).TotalMilliseconds -lt $shared.networkTimeout)
                    {
                        $OOBCmd = GetOOBCommand $tcp;
                        if($OOBCmd -ne 0) { break; }
                        Start-Sleep -Milliseconds 100;
                    }
                    if($OOBCmd -ne 'D' -and $OOBCmd -ne 'X') { throw "Could not repoint UND path in $($destLDF.physicalPath) file"; }

                    #close socket so destination will listen for another connection
                    CloseObjects $tcp $networkStream;
                    $tcp = $null;
                    $networkStream = $null;
                }

                #bring the database back online and wait for it to finish
                try
                {
                    $null = ExecuteDBCommand "ALTER DATABASE [$($meta.name)] SET ONLINE";
                }
                catch
                {
                    throw $_; #"Could not bring auto restore database online after copying MDF/LDF files";
                }

                #now restore all the incremental (log) files one by one in reverse order, from oldest to newest
                for($i=$fileDeltas.Count - 1; $i -ge 0; $i--)
                {
                    Write-Verbose "$($source.paths[0].path): Restoring incremental file: $($fileDeltas[$i].FullName)";
                    if($fileDeltas[$i].LastWriteTime -le $ToDateTime)
                    {
                        SendFile $fileDeltas[$i].FullName $newDbName;
                    }
                    else
                    {
                        #we are now 1 log backup file past the requested restore date/time. If this file does not contain any bulk entries, then we can do a point in time restore, otherwise it will error.
                        #Either way, we will be done after this...
                        SendFile $fileDeltas[$i].FullName $newDbName -stopAt $ToDateTime;
                        break;
                    }
                }

                #recover the database and then set the recovery type to simple
                #set recovery type
                $dbExecute = "RESTORE DATABASE [$($meta.name)] WITH CONTINUE_AFTER_ERROR,"
                switch($shared.recoveryType)
                {
                    "Recovery"
                    {
                        $dbExecute += "RECOVERY;" + 
                                      "ALTER DATABASE [$($meta.name)] SET RECOVERY SIMPLE"; #also set recovery type back to simple
                        break;
                    }

                    "NoRecovery"
                    {
                        $dbExecute += "NORECOVERY";
                        break;
                    }

                    "Standby"
                    {
                        $dbExecute += "STANDBY='$($destMDF.physicalPath)$(GenerateStandbyUndofilename $($meta.name))'";
                        break;
                    }
                }

                #run command and wait for database to be recovered
                #bring the database back online and wait for it to finish
                try
                {
                    $null = ExecuteDBCommand $dbExecute;
                }
                catch
                {
                    WriteErrorEx $_;
                    throw "Could not recover database after restore";
                }

                $shared.metrics.Success++;
            }
            catch
            {
                $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "11`r`n"); } catch { }
                WriteErrorEx $_;
            }
            finally
            {
                if($tcp -ne $null)
                {
                    CloseObjects $tcp $networkStream;
                    $tcp = $null;
                    $networkStream = $null;
                }
            }
        }
    }
    catch
    {
        WriteErrorEx $_;
    }
}

#connect to SQL service on local server and backup a single database
#NOTE: this does not use the CopyStream function, but instead uses code in the VDI to write into the stream.
#returns true/false if the source should continue running further backups, or stop (in case an abort was signalled)
function BackupDatabase
{
    param
    (
        [Parameter(Mandatory=$true)][string]$SQLServer,
        [Parameter(Mandatory=$true)][string]$dbName,
        [Parameter(Mandatory=$true)][ref]$continueWithBackups
    )

    $continueWithBackups.Value = $true; #return value, default is to continue

    $backupNotificationSent = $false;
    [Zstandard.Net.ZstandardStream]$zstd = $null;
    [Threading.Tasks.Task[Exception]]$task = $null;
    [VdiDotNet.VdiEngine]$vdi = $null;
    [Net.Sockets.NetworkStream]$networkStream = $null;
    [Net.Sockets.TcpClient]$tcp = $null;
    try
    {
        [string]$SQLCmd =  $null;
        [string]$SQLPreCmd =  $null;
        [string]$SQLPostCmd =  $null;
        [string]$UseDB = $null;
        [string]$instance = $null;
        [string]$undPath = $null;
        [bool]$isSystemDatabase = $false;
        [Exception]$DBError = $null;
        [System.Data.SqlClient.SqlConnection]$DB = $null;
        [System.Data.DataTable]$DT = $null;
        [System.Data.SqlClient.SqlDataAdapter]$DA = $null;
        [System.Data.SqlClient.SqlCommand]$CMD = $null;

        #create metadata of database to be backed up
        $meta = [MetaData]::new();
        $meta.name = $dbName;
        $meta.compressionLevel = $shared.compressionLevel;

        try
        {
            $DB = [System.Data.SqlClient.SqlConnection]::new("Server=$($SQLServer);Database=$($dbName);Trusted_Connection=true");
            $DB.Open();

            switch($dbName)
            {
                { $_ -in "master","model","msdb" } #system databases always use a full backup
                {
                    $meta.backupType = 0;
                    $isSystemDatabase = $true;
                    break;
                }

                "tempdb" #never backup the tempdb
                {
                    Write-Information "$($SQLServer)\$($dbName): Skipping tempdb Database";
                    $DB.Close();
                    return;
                    break;
                }

                default #all other databases need to be checked to determine if they can do an incremental or full backup
                {
                    #check to see if we can backup with the user's chosen BackupType
                    switch($shared.backupType)
                    {
                        "Full"
                        {
                            $meta.backupType = 0;
                            break;
                        }

                        "Differential"
                        {
                            $meta.backupType = 1;

                            if(!$shared.copyOnly)
                            {
                                $SQLCmd = "SELECT [is_read_only],[state] FROM sys.databases WHERE database_id=DB_ID('$($dbName)')";
                                $DT = [System.Data.DataTable]::new();
                                $DA = [System.Data.SqlClient.SqlDataAdapter]::new($SQLCmd, $DB);
                                [void]$DA.Fill($DT);
                                $DA.Dispose();
                                if($DT.Rows.Count -gt 0)
                                {
                                    if($DT.Rows[0]["is_read_only"] -eq 1)
                                    {
                                        $DT.Dispose();
                                        $DB.Close();
                                        Write-Information "$($SQLServer)\$($dbName): Skipping Database in 'Read-Only' state, use -CopyOnly parameter to run backup.";
                                        $shared.metrics.Skipped++;
                                        return;
                                    }
                                    if($DT.Rows[0]["state"] -eq 1)
                                    {
                                        $DT.Dispose();
                                        $DB.Close();
                                        Write-Information "$($SQLServer)\$($dbName): Skipping Database in 'Restoring' state.";
                                        $shared.metrics.Skipped++;
                                        return;
                                    }
                                }
                                $DT.Dispose();
                            }
                            break;
                        }

                        "Incremental"
                        {
                            $meta.backupType = 2;

                            $SQLCmd = "SELECT [recovery_model],[state],[is_read_only] FROM sys.databases WHERE database_id=DB_ID('$($dbName)')";
                            $DT = [System.Data.DataTable]::new();
                            $DA = [System.Data.SqlClient.SqlDataAdapter]::new($SQLCmd, $DB);
                            [void]$DA.Fill($DT);
                            $DA.Dispose();
                            if($DT.Rows.Count -gt 0)
                            {
                                if(!$shared.copyOnly) #do not care if we are read only for a copyonly
                                {
                                    if($DT.Rows[0]["is_read_only"] -eq 1)
                                    {
                                        $DT.Dispose();
                                        $DB.Close();
                                        Write-Information "$($SQLServer)\$($dbName): Skipping Database in 'Read-Only' state, use -CopyOnly parameter to run backup.";
                                        $shared.metrics.Skipped++;
                                        return;
                                    }
                                }

                                if($DT.Rows[0]["state"] -eq 1)
                                {
                                    $DT.Dispose();
                                    $DB.Close();
                                    Write-Information "$($SQLServer)\$($dbName): Skipping Database in 'Restoring' state.";
                                    $shared.metrics.Skipped++;
                                    return;
                                }

                                switch($DT.Rows[0]["recovery_model"])
                                {
                                    1 #full recovery model
                                    {
                                        if($shared.setIncrementalLoggingType -eq "Bulk_Logged")
                                        {
                                            Write-Verbose "$($SQLServer)\$($dbName): Switching recovery from Full to Bulk_Logged";
                                            $CMD = [System.Data.SqlClient.SqlCommand]::new("ALTER DATABASE [$($dbName)] SET RECOVERY $($shared.setIncrementalLoggingType) WITH NO_WAIT", $DB);
                                            $null = $CMD.ExecuteNonQuery();
                                            $CMD.Dispose();
                                        }
                                        break;
                                    }

                                    2 #bulk_logged recovery model
                                    {
                                        if($shared.setIncrementalLoggingType -eq "Full")
                                        {
                                            Write-Verbose "$($SQLServer)\$($dbName): Switching recovery from Bulk_Logged to Full";
                                            $CMD = [System.Data.SqlClient.SqlCommand]::new("ALTER DATABASE [$($dbName)] SET RECOVERY $($shared.setIncrementalLoggingType) WITH NO_WAIT", $DB);
                                            $null = $CMD.ExecuteNonQuery();
                                            $CMD.Dispose();
                                        }
                                        break;
                                    }

                                    default #simple recovery, switch to bulk, then run full backup (must run full before you can run a log backup)
                                    {
                                        Write-Verbose "$($SQLServer)\$($dbName): Database is set for Simple Recovery";
                                        if($shared.copyOnly)
                                        {
                                            $DT.Dispose();
                                            $DB.Close();
                                            Write-Error "$($SQLServer)\$($dbName): Omit -CopyOnly Parameter to switch database logging recovery and seed initial full backup";
                                            $shared.metrics.Skipped++;
                                            return;
                                        }

                                        Write-Verbose "$($SQLServer)\$($dbName): Switching recovery from Simple to $($shared.setIncrementalLoggingType) and seeding initial full backup";
                                        $CMD = [System.Data.SqlClient.SqlCommand]::new("ALTER DATABASE [$($dbName)] SET RECOVERY $($shared.setIncrementalLoggingType) WITH NO_WAIT", $DB);
                                        $null = $CMD.ExecuteNonQuery();
                                        $CMD.Dispose();
                                        $meta.backupType = 0;
                                        break;
                                    }
                                }
                            }
                            $DT.Dispose();
                            break;
                        }
                    }

                    #if differential/incremental, check for prior full backup, switch to full if never run
                    switch($meta.backupType)
                    {
                        1 #differential
                        {
                            $SQLCmd = "SELECT [differential_base_lsn] FROM sys.master_files WHERE database_id=DB_ID('$($dbName)') AND type=0";
                            $DT = [System.Data.DataTable]::new();
                            $DA = [System.Data.SqlClient.SqlDataAdapter]::new($SQLCmd, $DB);
                            [void]$DA.Fill($DT);
                            $DA.Dispose();
                            if($DT.Rows.Count -gt 0)
                            {
                                $lsn = $DT.Rows[0]["differential_base_lsn"];
                                if($lsn -is [DBNull])
                                {
                                    #oh oh, there hasn't been a full backup yet, see if we can switch to full backup
                                    Write-Verbose "$($SQLServer)\$($dbName): Database does not yet have an initial full backup";
                                    if($shared.copyOnly)
                                    {
                                        $DT.Dispose();
                                        $DB.Close();
                                        Write-Error "$($SQLServer)\$($dbName): Omit -CopyOnly parameter to seed initial full backup";
                                        $shared.metrics.Skipped++;
                                        return;
                                    }

                                    Write-Verbose "$($SQLServer)\$($dbName): Seeding initial full backup";
                                    $meta.backupType = 0;
                                    $meta.lastLSN = $null;
                                }
                                else
                                {
                                    $meta.lastLSN = $lsn;
                                }
                            }
                            $DT.Dispose();
                            break;
                        }

                        2 #incremental (log)
                        {
                            #if we are set to perform a log backup, let's make sure that there is already a full backup, or it will throw an error stating such
                            $SQLCmd = "SELECT [last_log_backup_lsn] FROM sys.database_recovery_status WHERE database_id = db_id('$($dbName)')";
                            $DT = [System.Data.DataTable]::new();
                            $DA = [System.Data.SqlClient.SqlDataAdapter]::new($SQLCmd, $DB);
                            [void]$DA.Fill($DT);
                            $DA.Dispose();
                            if($DT.Rows.Count -gt 0)
                            {
                                $lsn = $DT.Rows[0]["last_log_backup_lsn"];
                                if($lsn -is [DBNull])
                                {
                                    #oh oh, there hasn't been a full backup yet, see if we can switch to full backup
                                    Write-Verbose "$($SQLServer)\$($dbName): Database does not yet have an initial full backup";
                                    if($shared.copyOnly)
                                    {
                                        $DT.Dispose();
                                        $DB.Close();
                                        Write-Error "$($SQLServer)\$($dbName): Omit -CopyOnly parameter to seed initial full backup";
                                        $shared.metrics.Skipped++;
                                        return;
                                    }

                                    Write-Verbose "$($SQLServer)\$($dbName): Seeding initial full backup";
                                    $meta.backupType = 0;
                                    $meta.lastLSN = $null;
                                }
                                else
                                {
                                    $meta.lastLSN = $lsn;
                                }
                            }
                            $DT.Dispose();
                            break;
                        }
                    }
                    break;
                }
            }

            switch($shared.initialSeeding)
            {
                "OnlySeed"
                {
                    if($meta.backupType -ne 0)
                    {
                        Write-Information "$($SQLServer)\$($dbName): Skipping database, already seeded...";
                        $shared.metrics.Skipped++;
                        return;
                    }
                    break;
                }

                "NoSeed"
                {
                    if($meta.backupType -eq 0)
                    {
                        Write-Information "$($SQLServer)\$($dbName): Skipping database, needs initial seeded...";
                        $shared.metrics.Skipped++;
                        return;
                    }
                    break;
                }
            }

            #retrieve database's logical file info
            $SQLCmd = "SELECT [type],[name],[physical_name],[size],ISNULL(FILEPROPERTY([name],'SpaceUsed'),[size]) AS [used] FROM sys.master_files WHERE database_id=DB_ID('$($dbName)')";
            $DT = [System.Data.DataTable]::new();
            $DA = [System.Data.SqlClient.SqlDataAdapter]::new($SQLCmd, $DB);
            [void]$DA.Fill($DT);
            $DA.Dispose();
            if($DT.Rows.Count -gt 0)
            {
                $meta.files = [MetaFile[]]::new($DT.Rows.Count);
                for($i = 0; $i -lt $DT.Rows.Count; $i++)
                {
                    $meta.files[$i] = [MetaFile]::new();
                    $meta.files[$i].type = $DT.Rows[$i]["type"];
                    $meta.files[$i].logicalName = $DT.Rows[$i]["name"];
                    $meta.files[$i].physicalPath = $DT.Rows[$i]["physical_name"];
                    $meta.files[$i].size = $DT.Rows[$i]["size"] * 8192; #each page is 8k
                    $meta.files[$i].used = $DT.Rows[$i]["used"] * 8192; #each page is 8k

                    if($meta.files[$i].type -eq 0)
                    {
                        #use same path for UNDO file as the MDF file
                        $undPath = [IO.Path]::GetDirectoryName($meta.files[$i].physicalPath);
                        if (-not $undPath.EndsWith("\")) { $undPath += "\"; }
                    }
                }
            }
            $DT.Dispose();

            if($meta.files -eq $null) { throw "Database file information could not be retrieved."; }


            #if we are not in a system database, and we are not just running a -CopyOnly, then we need to make sure the POWERBAK_SOURCE database property is set
            if(!$isSystemDatabase -and !$shared.copyOnly -and !$shared.skipWritingDBProperty)
            {
                #if performing a source/destination role reversal, check the source database source property
                #otherwise create/update the database source property
                #NOTE: ignore if doing a -CopyOnly

                #find destination SQL Server name\instance (will stay $null if doing only a file backup)
                [string]$destSQLServer = $null;
                if($dest.paths[0].pathType -eq 0)
                {
                    $destSQLServer = $dest.paths[0].path;
                }
                elseif($dest.paths.length -gt 1 -and $dest.paths[1].pathType -eq 0)
                {
                    $destSQLServer = $dest.paths[1].path;
                }

                #retrieve existing database source property
                [string]$sourceProperty = $null;
                $SQLCmd = "SELECT value FROM fn_listextendedproperty('POWERBAK_SOURCE', default, default, default, default, default, default)";
                $DT = [System.Data.DataTable]::new();
                $DA = [System.Data.SqlClient.SqlDataAdapter]::new($SQLCmd, $DB);
                [void]$DA.Fill($DT);
                $DA.Dispose();
                if($DT.Rows.Count -gt 0)
                {
                    $sourceProperty = $DT.Rows[0][0];
                }
                $DT.Dispose();

                if ($shared.reverse)
                {
                    #if reversing the source/destination backup roles, make sure the sourceProperty is either the source or destination SQL server name\instance, or else skip this database
                    if([string]::IsNullOrEmpty($sourceProperty) -or [string]::IsNullOrEmpty($destSQLServer) -or ($sourceProperty -ne $SQLServer -and $sourceProperty -ne $destSQLServer))
                    {
                        Write-Information "$($SQLServer)\$($dbName): Skipping Database Role Reversal Because Database Source Property=$($sourceProperty)";
                        $DB.Close();
                        return;
                    }
                }
                else
                {
                    #if not reversing the source/destination backup roles, then just set the source database property
                    if([string]::IsNullOrEmpty($sourceProperty))
                    {
                        #create source database property
                        $CMD = [System.Data.SqlClient.SqlCommand]::new("sp_addextendedproperty 'POWERBAK_SOURCE', '$($SQLServer.Replace("'", "''"))'", $DB);
                        $null = $CMD.ExecuteNonQuery();
                        $CMD.Dispose();
                        Write-Information "$($SQLServer)\$($dbName): Created Database Property POWERBAK_SOURCE=$($SQLServer)";
                    }
                    else
                    {
                        #update source database property
                        #only update the property if it does not already equal either source or destination server name\instance
                        if($sourceProperty -ne $SQLServer -and ([string]::IsNullOrEmpty($destSQLServer) -or $sourceProperty -ne $destSQLServer))
                        {
                            $CMD = [System.Data.SqlClient.SqlCommand]::new("sp_updateextendedproperty 'POWERBAK_SOURCE', '$($SQLServer.Replace("'", "''"))'", $DB);
                            $null = $CMD.ExecuteNonQuery();
                            $CMD.Dispose();
                            Write-Information "$($SQLServer)\$($dbName): Updated Database Property POWERBAK_SOURCE OLD=$($sourceProperty) NEW=$($SQLServer)";
                        }
                    }
                }
            }
            $DB.Close();
            $DB = $null;
        }
        catch
        {
            $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "12`r`n"); } catch { }
            WriteErrorEx $_ -addToNotification "Database Backup Error: $($SQLServer)\$($dbName) -> ";

            if($DB -ne $null)
            {
                $DB.Dispose();
                $DB = $null;
            }

            return;
        }

        AddMetric $meta.backupType;

        #let's first see if we need to perform a DBCC CHECKDB on the destination, but if we are sending a differential/incremental backup type
        #i.e. if we're sending a full backup, there is no need to run a DBCC CHECKDB because the database is getting overwritten anyways
        if($shared.toCheckDB -and $meta.backupType -in 1,2)
        {
            try
            {
                $checkDBResult = ExecuteDBCommand "DBCC CHECKDB([$($dbName)])";
                if($checkDBResult -ne "0")
                {
                    #TODO: what to do with a checkdb error?  Reseed?
                    #      what if the same error exists on the source?
                    Write-Information "DBCC CHECKDB FAILED: $($checkDBResult)";
                }
                else
                {
                    Write-Information "DBCC SUCCEEDED: $($checkDBResult)";
                }
            }
            catch
            {
                throw "DBCC CheckDB Failed:";
            }
        }

        #perform the connection, backup, and transfer
        $backupComplete = $false;
        $streamCommand = 'S';
        while(!$backupComplete) #will only loop if the remote side asks us to stop and start over with a full backup
        {
            #close any previous connection (in case we loop because we are changing the backuptype)
            if($tcp -ne $null)
            {
                Write-Verbose "$($source.paths[0].path): BackupDatabase() - Reopening Objects";
                CloseObjects $tcp $networkStream $vdi $task $zstd;
                $tcp = $null;
                $networkStream = $null;
                $vdi = $null;
                $task = $null;
                $zstd = $null;
            }

            $abort = $false;
            $tcp,$networkStream = ConnectToDestinationServer $streamCommand $meta ([ref]$abort);
            if($abort)
            {
                #the far side has signalled us to stop backing up databases
                $shared.metrics.Skipped++; #do not increment fail counter, because technically nothing failed
                $backupNotificationSent = $true;
                $null = SendOOBCommand $tcp 'P'; #skiP
                $continueWithBackups.Value = $false;
                return;
            }

            #create backup command based on backuptype
            switch($meta.backupType)
            {
                0 #full
                {
                    Write-Information "$($SQLServer)\$($dbName): Running full backup$(if($shared.copyOnly) { " (Copy-Only)" })";
                    $SQLPreCmd = $null;
                    $SQLCmd = "BACKUP DATABASE [$($dbName)] TO VIRTUAL_DEVICE='{0}' WITH NO_COMPRESSION,CHECKSUM,MAXTRANSFERSIZE=$($shared.maxTransferSize),BUFFERCOUNT=$($shared.bufferCount)";
                    $SQLPostCmd = $null;
                    break;
                }

                1 #differential
                {
                    Write-Information "$($SQLServer)\$($dbName): Running differential backup$(if($shared.copyOnly) { " (Copy-Only)" })";
                    $SQLPreCmd = $null;
                    $SQLCmd = "BACKUP DATABASE [$($dbName)] TO VIRTUAL_DEVICE='{0}' WITH DIFFERENTIAL,NO_COMPRESSION,CHECKSUM,MAXTRANSFERSIZE=$($shared.maxTransferSize),BUFFERCOUNT=$($shared.bufferCount)";
                    $SQLPostCmd = $null;
                    break;
                }

                2 #incremental (log)
                {
                    Write-Information "$($SQLServer)\$($dbName): Running incremental backup$(if($shared.copyOnly) { " (Copy-Only)" })";
                    #$SQLPreCmd = "CHECKPOINT"; #commit as many transactions before backing up
                    $SQLPreCmd = $null;
                    $SQLCmd = "BACKUP LOG [$($dbName)] TO VIRTUAL_DEVICE='{0}' WITH NO_COMPRESSION,CHECKSUM,MAXTRANSFERSIZE=$($shared.maxTransferSize),BUFFERCOUNT=$($shared.bufferCount)";
                    $SQLPostCmd = $null;

                    #truncate the log file after incremental backup, but only if we aren't adding the WITH COPY_ONLY option, which would be a waste of time
                    if(!$shared.copyOnly)
                    {
                        foreach($f in $meta.files)
                        {
                            if($f.type -eq 1)
                            {
                                $SQLPostCmd = "DBCC SHRINKFILE('$($f.logicalName)', TRUNCATEONLY)";
                                break;
                            }
                        }
                    }
                    break;
                }
            }
            if($shared.copyOnly)
            {
                $SQLCmd += ",COPY_ONLY";
            }

            if(!$shared.reverse)
            {
                $UseDB = $dbName;
            }
            else
            {
                #this will assure that the database is inadvertantely locked preventing a tail log backup
                $SQLPreCmd = "ALTER DATABASE [$($meta.name)] SET ONLINE,SINGLE_USER WITH ROLLBACK IMMEDIATE;" + $SQLPreCmd;
                $SQLPostCmd = "ALTER DATABASE [$($meta.name)] SET ONLINE,MULTI_USER;" + $SQLPostCmd;
                #the backup must be run from the master database context if running a tail log backup (-Reverse flag)
                $UseDB = "master";

                #if we are reversing the primary/backup roles, then apply the RecoveryType to the database being backed up
                switch($shared.recoveryType)
                {
                    "Recovery"
                    {
                        $SQLCmd += ",RECOVERY";
                        break;
                    }

                    "NoRecovery"
                    {
                        $SQLCmd += ",NORECOVERY";
                        break;
                    }

                    "Standby"
                    {
                        if([string]::IsNullOrEmpty($undPath))
                        {
                            #get SQL Default paths in case undo path is empty (shouldn't happen)
                            $instance = ($SQLServer -split "\\")[1];
                            [string]$logPath = null;
                            $undPath,$logPath = GetDefaultSQLPaths $instance;
                        }

                        $SQLCmd += ",STANDBY='$($undPath)$(GenerateStandbyUndoFilename $meta.name)'"; #include 60 padding so we don't overflow the buffer when we replace the UND path in the LDF file during an auto restore
                        break;
                    }
                }
            }

            Write-Verbose "$($SQLServer)\$($dbName): Streaming backup to destination...";

            #add compression to the stream if the compression level is greater than 0
            if($meta.CompressionLevel -gt 0)
            {
                $zstd = [Zstandard.Net.ZstandardStream]::new($networkStream, [IO.Compression.CompressionMode]::Compress, $true);
                $zstd.CompressionLevel = $meta.compressionLevel;
                $task,$vdi = StreamSQLCommand $SQLServer $UseDB $SQLPreCmd $SQLCmd $SQLPostCmd $zstd;
            }
            else
            {
                $task,$vdi = StreamSQLCommand $SQLServer $UseDB $SQLPreCmd $SQLCmd $SQLPostCmd $networkStream;
            }

            #Loop while we wait for the backup to complete
            #$closeStreamWhenDone = $false;
            $taskEnded = [DateTime]::MinValue;
            $backupNotificationSent = $false;

            while($true) #task wait loop
            {
                #the sleep must go before the OOB check and then the task check
                #if it goes after the OOB check, it's possible that the OOB will be missed after a successful backup, the network socket will already be closed,
                #and a database backup might be mistakenly marked as failed
                Start-Sleep -Milliseconds 50;

#Write-Verbose "$($SQLServer)\$($dbName): Checking for feedback from destination";

                $OOBCmd = GetOOBCommand $tcp;
                if($OOBCmd -ne 0)
                {
                    Write-Verbose "$($SQLServer)\$($dbName): Received Status Message From Destination: $OOBCmd";

                    switch($OOBCmd)
                    {
                        'S' #Retry
                        {
                            #something has gone wrong on the destination, but it wants us to unconiditionally retry (SQL volume went offline?)
                            $vdi.AbortStream();
                            #we are aborting, ignore task exceptions
                            $tasks = [Threading.Tasks.Task[]]::new(1);
                            $tasks[0] = $task;
                            if([System.Threading.Tasks.Task]::WaitAny($tasks, $shared.networkTimeout) -eq -1) { Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Cannot Abort VDI Task!?"; }
                            #if(!$task.Wait($shared.networkTimeout)) { Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Cannot Abort VDI Task!?"; }

                            $DBError = $null; #make sure DBError is null because we don't want to falsely log the vdi.AbortStream( )

                            #retry backup
                            Write-Information "$($SQLServer)\$($dbName): Retrying backup";
                            $shared.metrics.Retry++;

                            #do NOT break out of the backup loop because we are sending the backup again
                            #$backupComplete = $true;

                            if(!$backupNotificationSent)
                            {
                                $backupNotificationSent = $true;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "113`r`n"); } catch { }
                                $null = SendOOBCommand $tcp 'A'; #Abort current run
                            }

                            break;
                        }

                        'F' #ull (reseed)
                        {
                            #something has gone wrong on the destination, but it has detected that switching to a FULL backup will fix the problem...
                            $vdi.AbortStream();
                            #make sure task is stopped, ignoring any exceptions it throws
                            $tasks = [Threading.Tasks.Task[]]::new(1);
                            $tasks[0] = $task;
                            if([System.Threading.Tasks.Task]::WaitAny($tasks, $shared.networkTimeout) -eq -1) { Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Cannot Abort VDI Task!?"; }
                            #if(!$task.Wait($shared.networkTimeout)) { Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Cannot Abort VDI Task!?"; }

                            $DBError = $null; #make sure DBError is null because we don't want to falsely log the vdi.AbortStream( )

                            if($meta.backupType -ne 0)
                            {
                                AddMetric $meta.backupType -1 #subtract the previous backup type from metric counts, since it did not succeed

                                if($shared.reverse)
                                {
                                    #do not switch to a full database backup if running a role reversal, just exit the loop because something went wrong
                                    $shared.metrics.Failure++;
                                    $backupComplete = $true;
                                }
                                else
                                {
                                    #switch to full backup
                                    Write-Information "$($SQLServer)\$($dbName): Switching to full database backup";
                                    $meta.backupType = 0;
                                    $shared.metrics.Reseed++;
                                    $meta.lastLSN = $null;

                                    #do NOT break out of the backup loop because we are sending the backup again
                                    #$backupComplete = $true;
                                }
                            }
                            else
                            {
                                $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "13`r`n"); } catch { }
                                Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Requested to send a full backup, but we are already sending a full backup!?";

                                #break out of the backup loop
                                $backupComplete = $true;
                            }

                            if(!$backupNotificationSent)
                            {
                                $backupNotificationSent = $true;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "114`r`n"); } catch { }
                                $null = SendOOBCommand $tcp 'A'; #Abort current run
                            }

                            break;
                        }

                        'T' #Retry without OPTION RESTART
                        {
                            #something has gone wrong on the destination, but it has detected that it can fix it, so we need to retry the backup
                            $vdi.AbortStream();
                            #we are aborting, ignore task exceptions
                            $tasks = [Threading.Tasks.Task[]]::new(1);
                            $tasks[0] = $task;
                            if([System.Threading.Tasks.Task]::WaitAny($tasks, $shared.networkTimeout) -eq -1) { Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Cannot Abort VDI Task!?"; }
                            #if(!$task.Wait($shared.networkTimeout)) { Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Cannot Abort VDI Task!?"; }

                            $DBError = $null; #make sure DBError is null because we don't want to falsely log the vdi.AbortStream( )

                            if($streamCommand -ne 'T')
                            {
                                #retry backup
                                $streamCommand = 'T';
                                Write-Information "$($SQLServer)\$($dbName): Retrying backup without OPTION RESTART";
                                $shared.metrics.Retry++;

                                #do NOT break out of the backup loop because we are sending the backup again
                                #$backupComplete = $true;
                            }
                            else
                            {
                                $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "14`r`n"); } catch { }
                                Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Requested to retry backup without RESTART, but we already did!?";

                                #break out of the backup loop
                                $backupComplete = $true;
                            }

                            if(!$backupNotificationSent)
                            {
                                $backupNotificationSent = $true;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "115`r`n"); } catch { }
                                $null = SendOOBCommand $tcp 'A'; #Abort current run
                            }

                            break;
                        }

                        'U' #Retry without OPTION CONTINUE_AFTER_ERROR
                        {
                            #something has gone wrong on the destination, but it has detected that it can fix it, so we need to retry the backup
                            $vdi.AbortStream();
                            #we are aborting, ignore task exceptions
                            $tasks = [Threading.Tasks.Task[]]::new(1);
                            $tasks[0] = $task;
                            if([System.Threading.Tasks.Task]::WaitAny($tasks, $shared.networkTimeout) -eq -1) { Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Cannot Abort VDI Task!?"; }
                            #if(!$task.Wait($shared.networkTimeout)) { Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Cannot Abort VDI Task1!?"; }

                            $DBError = $null; #make sure DBError is null because we don't want to falsely log the vdi.AbortStream( )

                            if($streamCommand -ne 'U')
                            {
                                #retry backup
                                $streamCommand = 'U';
                                Write-Information "$($SQLServer)\$($dbName): Retrying backup with OPTION CONTINUE_AFTER_ERROR";
                                $shared.metrics.Retry++;

                                #do NOT break out of the backup loop because we are sending the backup again
                                #$backupComplete = $true;
                            }
                            else
                            {
                                $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "15`r`n"); } catch { }
                                Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Requested to retry backup without RESTART, but we already did!?";

                                #break out of the backup loop
                                $backupComplete = $true;
                            }

                            if(!$backupNotificationSent)
                            {
                                $backupNotificationSent = $true;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "116`r`n"); } catch { }
                                $null = SendOOBCommand $tcp 'A'; #Abort current run
                            }

                            break;
                        }

                        { $_ -in 'A','P' } #Abort, skiP
                        {
                            #the destination does not like what they got, nor do they want us to try to fix it, abort the backup and exit the loop
                            if($OOBCmd -eq 'A')
                            {
                                $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "16`r`n"); } catch { }
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "117`r`n"); } catch { }
                            }
                            else
                            {
                                $shared.metrics.Skipped++;
                            }

                            Write-Information "$($SQLServer)\$($dbName): Cancelling database backup";

                            $vdi.AbortStream();
                            #make sure task is stopped, ignoring any exceptions it throws
                            $tasks = [Threading.Tasks.Task[]]::new(1);
                            $tasks[0] = $task;
                            if([System.Threading.Tasks.Task]::WaitAny($tasks, $shared.networkTimeout) -eq -1) { Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Cannot Abort VDI Task!?"; }
                            #if(!$task.Wait($shared.networkTimeout)) { Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Cannot Abort VDI Task!?"; }

                            $DBError = $null; #make sure DBError is null because the destination will log the error (no need to double log)

                            #break out of the backup loop
                            $backupComplete = $true;

                            if(!$backupNotificationSent)
                            {
                                $backupNotificationSent = $true;
                                $null = SendOOBCommand $tcp $OOBCmd; #Abort/skip current backup
                            }

                            break;
                        }

                        { $_ -in 'D', 'X' } #Done success
                        {
                            if($OOBCmd -eq 'X')
                            {
                                #the far side has signalled us to stop backing up databases after this one
                                $continueWithBackups.Value = $false;
                            }

                            Write-Information "$($SQLServer)\$($dbName): Confirming database backup";
                            #allow the VDI to exit, notifying SQL that we had a successful backup, otherwise the last backup date won't update, the log won't truncate, a full backup won't reset the DCB, etc.. etc...
                            [bool]$failed = $false;
                            $vdi.ConfirmBackup();
                            if($task.Wait($shared.networkTimeout)) #wait for VDI to finish, use $task.Wait( ) which will throw an exception if the task throws an exception
                            {
                                #store the backup results, see if it failed
                                $DBError = $task.Result;
                                if(1 -in (CheckDBError $DBError))
                                {
                                    $failed = $true;
                                }
                            }
                            else
                            {
                                $failed = $true;
                                Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> VDI Task Not Exiting After Successful Backup!?";
                            }

                            if($failed)
                            {
                                $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "17`r`n"); } catch { }
                                Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> **** ALERT **** THE RESTORE SUPPOSEDLY SUCCEEDED, BUT WE HAVE AN ERROR ON THE BACKUP SIDE, THIS SHOULD NOT HAPPEN??";
                            }
                            else
                            {
                                $shared.metrics.Success++;
                            }

                            #break out of the backup loop
                            $backupComplete = $true;

                            if(!$backupNotificationSent)
                            {
                                $backupNotificationSent = $true;
                                if($failed)
                                {
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "118`r`n"); } catch { }
                                    $null = SendOOBCommand $tcp 'A'; #bort
                                }
                                else
                                {
                                    $null = SendOOBCommand $tcp 'D'; #Done
                                }
                            }

                            break;
                        }
                    }
                    break; #always break out of the task wait loop when we receive an OOB message
                }

                $isTaskComplete = $task.IsCompleted;
                #note that a task can remain incomplete after the $vdi.StreamComplete = true, as it will wait for Backup Confirmation or Abort
                if(!$backupNotificationSent -and ($isTaskComplete -or $vdi.StreamComplete))
                {
                    $backupNotificationSent = $true; #only send backup notification once

                    if($zstd -ne $null)
                    {
                        #if using compression, close the compression stream which will guarantee the far end will stop waiting for bytes
                        try { $zstd.Close(); } catch { }
                    }

                    if($isTaskComplete)
                    {
                        #only check the $task.Result if the task has completed, otherwise we'll hang
                        #normally when a backup is finished, the VDI task will wait for confirmation from us before it finishes
                        #so chances are if the task completed here, an error has occurred during the backup we'll need to send an Abort message to the destination
                        #Start-Sleep -milliseconds 100;
                        $DBError = $task.Result;
                        if(1 -in (CheckDBError $DBError)) #1=failed
                        {
                            WriteErrorEx $DBError -addToNotification "Database Backup Error: $($SQLServer)\$($dbName) -> ";
                            #notify far end that something went wrong and we are done sending
                            $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "18`r`n"); } catch { }
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "119`r`n"); } catch { }
                            $null = SendOOBCommand $tcp 'A'; #Abort

                            #break out of the backup loop
                            $backupComplete = $true;
                            break;
                        }
                    }

                    #send OOB done command so the other side knows we are done sending
                    if((SendOOBCommand $tcp 'D') -eq $false)
                    {
                        #if the OOB send failed (far side either sent an abort and disconnected, or the network hiccupped)
                        #break out of the backup loop
                        $vdi.AbortStream();
                        #make sure task is stopped, ignoring any exceptions it throws
                        $tasks = [Threading.Tasks.Task[]]::new(1);
                        $tasks[0] = $task;
                        if([System.Threading.Tasks.Task]::WaitAny($tasks, $shared.networkTimeout) -eq -1) { Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Cannot Abort VDI Task!?"; }
                        #$null = $task.Wait($shared.networkTimeout);

                        $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "19`r`n"); } catch { }
                        Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Backup Done Sending, But Destination Did Not Confirm, Calling Abort";
                        $backupComplete = $true; #exit backup loop
                        break;
                    }
                }

                if($isTaskComplete)
                {
                    #if the streaming task is complete, make sure we don't hit a timeout waiting for the destination to respond
                    if($taskEnded -eq [DateTime]::MinValue) { $taskEnded = [DateTime]::Now; }

                    if(([DateTime]::Now - $taskEnded).TotalMilliseconds -gt $shared.networkTimeout)
                    {
                        $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "20`r`n"); } catch { }
                        Write-Error "Database Backup Error: $($SQLServer)\$($dbName) -> Timeout Waiting For Destination Response, VDI StreamComplete=$($vdi.StreamComplete)";
                        $backupComplete = $true;
                        break;
                    }
                }
            }
            Write-Verbose "$($SQLServer)\$($dbName): Streaming complete";

            #the DBError variable will already be set up from the above loop, so we can simply check to see if we need to log it. Do not pull the $task.result, or we may log false positives.
            #$DBError = $task.Result;

            if(-1 -in (CheckDBError $DBError))
            {
                if($DBError.GetType() -eq [System.AggregateException])
                {
                    WriteErrorAgg $DBError -addToNotification "Database Backup Error: $($SQLServer)\$($dbName) -> ";
                }
                else
                {
                    WriteErrorEx $DBError -addToNotification "Database Backup Error: $($SQLServer)\$($dbName) -> ";
                }
            }
        }
    }
    catch
    {
        if($_.GetType() -eq [System.AggregateException])
        {
            WriteErrorAgg $_ -addToNotification "Database Backup Error: $($SQLServer)\$($dbName) -> ";
        }
        else
        {
            WriteErrorEx $_ $task -addToNotification "Database Backup Error: $($SQLServer)\$($dbName) -> ";
        }
        #throw;
    }
    finally
    {
        #Write-Verbose "$($SQLServer)\$($dbName): Exiting BackupDatabase"
        if($tcp -ne $null -and !$backupNotificationSent) #send abort backup notification if we haven't sent a notification yet (should only happen if an exception was thrown while streaming the backup)
        {
            $backupNotificationSent = $true;
            $shared.metrics.Failure++;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "21`r`n"); } catch { }
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "120`r`n"); } catch { }
            $null = SendOOBCommand $tcp 'A'; #Abort
            if($vdi -ne $null) { $vdi.AbortStream(); } #we probably don't need this, because the vdi.Dispose will make sure this is called
        }

        Write-Verbose "$($SQLServer)\$($dbName): ::BackupDatabase: Closing Objects";
        CloseObjects $tcp $networkStream $vdi $task $zstd;
        $networkStream = $null;
        $tcp = $null;
        $vdi = $null;
        $task = $null;
        $zstd = $null;
    }
}

#setup a restore of a single database from the passed in stream
function RestoreDatabase
{
    [OutputType([Threading.Tasks.Task[Exception]],[VdiDotNet.VdiEngine])]
    param
    (
        [Parameter(Mandatory=$true)][string]$SQLServer, #if there is an instance, include server\instancename
        [Parameter(Mandatory=$true)][MetaData]$meta,
        [Parameter(Mandatory=$true)][IO.Stream]$stream,
        [Parameter()][datetime]$stopAt = [datetime]::MaxValue,
        [Parameter()][bool]$disableRestartOption = $false,
        [Parameter()][bool]$enableContinueAfterErrorOption = $false,
        [Parameter()][string]$dbName = $null #if there is a preferred dbName, include it here, otherwise it will use the $meta.name
    )

    try
    {
        [string]$SQLCmd = $null;
        [string]$instance = $null;
        [string]$dataPath = $null;
        [string]$logPath = $null;

        #find our SQL instance name, default data/log paths, and database name
        $instance = ($SQLServer -split "\\")[1];
        $dataPath,$logPath = $shared.overrideSQLPaths;
        if([string]::IsNullOrEmpty($dataPath) -or [string]::IsNullOrEmpty($logPath))
        {
            [string]$tmpDataPath = $null;
            [string]$tmpLogPath = $null;
            $tmpDataPath,$tmpLogPath = GetDefaultSQLPaths $instance;
            if([string]::IsNullOrEmpty($dataPath)) { $dataPath = $tmpDataPath; }
            if([string]::IsNullOrEmpty($logPath)) { $logPath = $tmpLogPath; }
        }
        if([string]::IsNullOrEmpty($dbName))
        {
            $dbName = $meta.name;
        }

        #put together restore SQL command
        if($($meta.backupType) -eq 2)
        {
            $SQLCmd = "RESTORE LOG [$($dbName)] FROM VIRTUAL_DEVICE='{0}' WITH MAXTRANSFERSIZE=$($shared.maxTransferSize),BUFFERCOUNT=$($shared.bufferCount)";
            if($stopAt -lt [datetime]::MaxValue) { $SQLCmd += ",STOPAT='$($stopAt.ToString('yyyy-MM-ddTHH:mm:ss'))'"; }
        }
        else
        {
            $SQLCmd = "RESTORE DATABASE [$($dbName)] FROM VIRTUAL_DEVICE='{0}' WITH MAXTRANSFERSIZE=$($shared.maxTransferSize),BUFFERCOUNT=$($shared.bufferCount)";
            #perform MOVE on all logical filenames when performing a full restore, or they will try and restore to the original locations
            for($i = 0; $i -lt $meta.files.Length; $i++)
            {
                if($meta.files[$i].type -eq 0)
                {
                    $SQLCmd += ",MOVE '$($meta.files[$i].logicalName)' TO '$($dataPath)$($dbName).mdf'";
                }
                else
                {
                    $SQLCmd += ",MOVE '$($meta.files[$i].logicalName)' TO '$($logPath)$($dbName).ldf'";
                }
            }
        }
        if($shared.reverse)
        {
            #when reversing the primary/backup roles, recover the backup database after restoring the tail log file
            #the primary database being backed up will have the RecoveryType applied to it
            $SQLCmd += ",RECOVERY";
        }
        else
        {
            switch($shared.recoveryType)
            {
                "Recovery"
                {
                    $SQLCmd += ",RECOVERY";
                    break;
                }

                "NoRecovery"
                {
                    $SQLCmd += ",NORECOVERY";
                    break;
                }

                "Standby"
                {
                    $SQLCmd += ",STANDBY='$($dataPath)$(GenerateStandbyUndoFilename $dbName)'"; #include 60 padding so we don't overflow the buffer when we replace the UND path in the LDF file during an auto restore
                    break;
                }
            }
        }
        if($shared.replace) { $SQLCmd += ",REPLACE"; }
        if($shared.continueAfterError -or $enableContinueAfterErrorOption) { $SQLCmd += ",CONTINUE_AFTER_ERROR"; }
        if(!$disableRestartOption -and $meta.backupType -ne 0) { $SQLCmd += ",RESTART"; } #add RESTART option if we are not performing a full backup/restore, and it has not been disabled

        #this will assure that the database is inadvertantely locked preventing the restore from running
        $SQLPreCmd = "ALTER DATABASE [$($dbName)] SET ONLINE,SINGLE_USER WITH ROLLBACK IMMEDIATE";
        $SQLPostCmd = "ALTER DATABASE [$($dbName)] SET ONLINE,MULTI_USER";

        #the restore must be run from the master database context
        return StreamSQLCommand $SQLServer "master" $SQLPreCmd $SQLCmd $SQLPostCmd $stream;
    }
    catch
    {
        #WriteErrorEx $_;
        throw;
    }
}

#We need to create a standby filename that can withstand renames in an LDF without overflowing the path
#For example, during a restore, we rename the old UND path to the new UND (the new UND filename will not have any padding), so in case the new path is longer than the old path, we need the padding so we don't overflow!
function GenerateStandbyUndoFilename
{
    [OutputType([string])]
    param
    (
        [Parameter(Mandatory=$true)][string]$dbName
    )

    return "$($dbName)____________________________________________________________.UND";
}

<#
RESTORE DATABASE successfully processed 1234 pages in 1.234 seconds (1.234 MB/sec).
RESTORE LOG successfully processed 1234 pages in 1.234 seconds (1.234 MB/sec).
Could not insert a backup or restore history/detail record in the MyDatabase database. This may indicate a problem with the MyDatabase database. The backup/restore operation was still successful.
Failed to update database "MyDatabase" because the database is read-only.
Database 'MyDatabase' cannot be opened because it is offline.
The database "MyDatabase" does not exist. RESTORE can only create a database when restoring either a full backup or a file backup of the primary file.
The log in this backup set terminates at LSN 555000000005800001, which is too early to apply to the database. A more recent log backup that includes LSN 555000000008900001 can be restored.
The log in this backup set begins at LSN 555000000070800001, which is too recent to apply to the database. An earlier log backup that includes LSN 555000000069900001 can be restored.
The log or differential backup cannot be restored because no files are ready to rollforward.
This log cannot be restored because a gap in the log chain was created. Use more recent data backups to bridge the gap.
The backup set holds a backup of a database other than the existing 'MyDatabase' database.
Exclusive access could not be obtained because the database is in use.
File "LogicalFilename_Data" cannot be restored over the existing "C:\Backups\MyDatabase.mdf". Reissue the RESTORE statement using WITH REPLACE to overwrite pre-existing files, or WITH MOVE to identify an alternate location.
A previous restore operation was interrupted and did not complete processing on file 'LogicalFilename_Data'. Either restore the backup set that was interrupted or restart the restore sequence.
The restart-checkpoint file 'RestoreCheckpointDB123.CKP' is from a previous interrupted RESTORE operation and is inconsistent with the current RESTORE command.
  The restart command must use the same syntax as the interrupted command, with the addition of the RESTART clause.
  Alternatively, reissue the current statement without the RESTART clause.
The operation did not proceed far enough to allow RESTART. Reissue the statement without the RESTART qualifier.
SQL Server detected a logical consistency-based I/O error: incorrect pageid (expected 1:00000000; actual 1:11111111).
SQL Server detected a logical consistency-based I/O error: invalid protection option. It occurred during a read of page (1:12780807) in database ID 1063 at offset 0x00001860a0e000 in file
  'E:\CURRENT_DATA\2014\caduceushealthcare.mdf'.  Additional messages in the SQL Server error log or system event log may provide more
  detail. This is a severe error condition that threatens database integrity and must be corrected immediately. Complete a full database
  consistency check (DBCC CHECKDB). This error can be caused by many factors; for more information, see SQL Server Books Online.
  RESTORE LOG is terminating abnormally.
SQL Server detected a logical consistency-based I/O error: torn page (expected signature: 0x55555555; actual signature: 0xdf604081).
  It occurred during a read of page (1:35603) in database ID 379 at offset 0x00000011626000 in file 'MyDatabase.mdf'.
  Additional messages in the SQL Server error log or system event log may provide more detail.
  This is a severe error condition that threatens database integrity and must be corrected immediately.
  Complete a full database consistency check (DBCC CHECKDB).
This error can be caused by many factors; for more information, see SQL Server Books Online.
RESTORE LOG is terminating abnormally.
The restart-checkpoint file '\MSSQL\Backup\RestoreCheckpointDBXXX.CKP' was not found. The RESTORE command will continue from the beginning as if RESTART had not been specified.
The operating system returned error 1167(The device is not connected.) to SQL Server during a read at offset 0x00000000002000 in file 'xxx.ldf'.
  Additional messages in the SQL Server error log and system event log may provide more detail.
  This is a severe system-level error condition that threatens database integrity and must be corrected immediately.
  Complete a full database consistency check (DBCC CHECKDB).
Could not redo log record (44530:687:35), for transaction ID (0:0), on page (1:65477), database 'MyDatabase' (database ID 580).
  Page: LSN = (44530:687:25), type = 1. Log: OpCode = 18, context 2, PrevPageLSN: (44516:254:58).
  Restore from a backup of the database, or repair the database.
The log scan number (25475:496:1) passed to log scan in database 'MyDatabase' is not valid.
  This error may indicate data corruption or that the log file (.ldf) does not match the data file (.mdf).
  If this error occurred during replication, re-create the publication.
  Otherwise, restore from backup if the problem results in a failure during startup.
An error occurred while processing the log for database 'MyDatabase'.
  If possible, restore from backup. If a backup is not available, it might be necessary to rebuild the log.
A previous RESTORE WITH CONTINUE_AFTER_ERROR operation left the database in a potentially damaged state.
  To continue this RESTORE sequence, all further steps must include the CONTINUE_AFTER_ERROR option.
An error occurred during recovery, preventing the database 'MyDatabase' (database ID 123) from restarting.
  Diagnose the recovery errors and fix them, or restore from a known good backup.
  If errors are not corrected or expected, contact Technical Support.
The operating system returned error 38(Reached the end of the file.) to SQL Server during a read at offset 0x00000000490000 in file 'C:\...\MyDatabase.ldf'.
  Additional messages in the SQL Server error log and system event log may provide more detail.
  This is a severe system-level error condition that threatens database integrity and must be corrected immediately.
  Complete a full database consistency check (DBCC CHECKDB).
Cannot open database "MyDatabase" requested by the login. The login failed.
  Login failed for user 'DOMAIN\Username'.
This backup set cannot be applied because it is on a recovery path that is inconsistent with the database.
  The recovery path is the sequence of data and log backups that have brought the database to a particular recovery point.
  Find a compatible backup to restore, or restore the rest of the database to match a recovery point within this backup set,
  which will restore the databse to a different point in time.
VDI OpenDevice Failed.  HRESULT: 80770004
Database 855 cannot be autostarted during server shutdown or startup.
Primary log file is not available for database 'MyDatabase' (xxx:0).
A system assertion check has failed. Check the SQL Server error log for details. Typically, an assertion failure is caused by a software bug or data corruption. To check for database corruption, consider running DBCC CHECKDB. If you agreed to send dumps to Microsoft during setup, a mini dump will be sent to Microsoft. An update might be available from Microsoft in the latest Service Pack or in a QFE from Technical Support.
A network-related or instance-specific error occurred while establishing a connection to SQL Server. The server was not found or was not accessible. Verify that the instance name is correct and that SQL Server is configured to allow remote connections. (provider: Named Pipes Provider, error: 40 - Could not open a connection to SQL Server)
During startup of warm standby database 'worthyworksmail' (database ID 944), its standby file ('C:\MyBackups\MyDatabase\UND') was inaccessible to the RESTORE statement. The operating system error was '2(The system cannot find the file specified.)'. Diagnose the operating system error, correct the problem, and retry startup.
RESTORE cannot apply this backup set because the database is suspect. Restore a backup set that repairs the damage.
RESTORE detected an error on page (0:0) in database "MyDatabase" as read from the backupset.
RESTORE DATABASE is terminating abnormally.
RESTORE LOG is terminating abnormally.
The statement has been terminated.

A nonrecoverable I/O error occurred on file "9EF1C0FA-E50E-4349-97C6-52B66839E0A8:" 995(The I/O operation has been aborted because of either a thread exit or an application request.).
A nonrecoverable I/O error occurred on file "1CEA71B0-E1F8-4858-BA9A-736979F3AA91:" 39(The disk is full.).
A nonrecoverable I/O error occurred on file "1CEA71B0-E1F8-4858-BA9A-736979F3AA91:" 1460(This operation returned because the timeout period expired.).
#>

#This will determine the nature of any resulting database error and decide if it can be auto-corrected, logged, completed, failed, etc...
#one or more actions can be returned in an array (i.e. -1,1,11 = log for email notification, failed, reseed full backup)
# Return values:
# -2 = skip database
# -1 = include in log
#  0 = success
#  1 = failed
# 11 = restart with reseed full backup
# 12 = restart without RESTART option
# 13 = restart with CONTINUE_AFTER_ERROR option
# 14 = restart, no changes (most likely VSS volume error or SQL is offline)
function CheckDBError
{
    [OutputType([int[]])]
    param
    (
        [Parameter()][Exception]$DBError
    )

    if($DBError -eq $null) { return (0); }

    switch -Regex ($DBError.Message)
    {
        "^Skip:"
        {
            #normal, comes from an intentional skip, do not log or throw message (i.e. system database restore)
            return (-2);
        }

        "^Internal:"
        {
            #comes from the backup source telling us an error has occurred, there's nothing for us to do but exit and move on, the source will log the issue
            if($DBError.Message.Length -gt 10) { Write-Verbose $DBError.Message.Substring(10); }
            return (1);
        }

        "I/O operation has been aborted because of either a thread exit or an application request"
        {
            #normal, comes from an intentional abort request, do not log or throw message
            return (1);
        }

        "VD_E_ABORT"
        {
            #normal, comes from an intentional abort request, do not log or throw message
            return (1);
        }

        "RESTORE (?:DATABASE|LOG) successfully processed"
        {
            #Hooray
            return (0);
        }

        "failed: 1167\("
        {
            #retry same database
            return (-1,1,14);
        }

        "The operating system returned error (1167|21)\("
        {
            #retry same database
            return (-1,1,14);
        }

        "Primary log file is not available for database"
        {
            #database server is offline, retry
            return (-1,1,14);
        }

        "Database 855 cannot be autostarted during server shutdown or startup"
        {
            #database server is restarting, retry
            return (-1,1,14);
        }

        "A network-related or instance-specific error occurred while establishing a connection to SQL Server"
        {
            #database server is restarting, retry
            return (-1,1,14);
        }

        "The operating system returned error (23|38)\("
        {
            #corruption
            return (-1,1,11);
        }

        "The database .*? does not exist"
        {
            #Requesting full backup
            return (1,11);
        }

        "The log in this backup set (begins|terminates) at LSN"
        {
            #Requesting full backup, include in log
            return (-1,1,11);
        }

        "This backup set cannot be applied because it is on a recovery path that is inconsistent with the database."
        {
            #Requesting full backup, include in log
            return (-1,1,11);
        }

        "The log scan number .*? passed to log scan in database .*? is not valid."
        {
            #Requesting full backup, include in log
            return (-1,1,11);
        }

        "gap in the log chain was created"
        {
            #Requesting full backup, include in log
            return (-1,1,11);
        }

        "The log or differential backup cannot be restored because no files are ready to rollforward"
        {
            #Requesting full backup, include in log
            return (-1,1,11);
        }

        "The backup set holds a backup of a database other than the existing .*? database"
        {
            #Requesting full backup, include in log
            return (-1,1,11);
        }

        "Either restore the backup set that was interrupted or restart the restore sequence"
        {
            #Requesting full backup
            return (1,11);
        }

        "RESTORE detected an error on page"
        {
            #Requesting full backup
            return (-1,1,11);
        }

        "RESTORE cannot apply this backup set because the database is suspect"
        {
            #Requesting full backup, include in log
            return (-1,1,11);
        }

        "During startup of warm standby database .*? The operating system error was '2\(The system cannot find the file specified"
        {
            #the undo portion of the warm standby may have been deleted, we need to reseed
            return (-1,1,11);
        }

        "A previous RESTORE WITH CONTINUE_AFTER_ERROR"
        {
            Write-Verbose "$($source.paths[0].path): Switching on OPTION CONTINUE_AFTER_ERROR";
            return (1,13);
        }

        "I/O error: torn page"
        {
            #Write-Verbose "$($source.paths[0].path): Torn Page!?!? What The Hell";
            #Requesting full backup, include in log
            return (-1,1,11);
        }

        "I/O error: incorrect pageid"
        {
            #Write-Verbose "$($source.paths[0].path): Happens during power outtage??";
            #Requesting full backup, include in log
            return (-1,1,11);
        }

       "I/O error \(torn page\) detected"
        {
            #Write-Verbose "$($source.paths[0].path): Torn Page!?!? What The Hell";
            #Requesting full backup, include in log
            return (-1,1,11);
        }

        "The restart command must use the same syntax as the interrupted command"
        {
            Write-Verbose "$($source.paths[0].path): Switching off OPTION RESTART";
            return (1,12);
        }

        "Reissue the statement without the RESTART qualifier"
        {
            Write-Verbose "$($source.paths[0].path): Switching Off RESTART Option";
            return (1,11); #if a database is completely missing, it will show this error, so switch to a full restore
            #return (1,12);
        }

        "Database .*? cannot be opened because it is offline"
        {
#TODO: Should there be an option to automatically set it online/drop if not able to, and try again?
            return (-1,1);
        }

        "A nonrecoverable I/O error occurred on file .*? The disk is full"
        {
            #this can't be good
            return (-1,1);
        }

        "A nonrecoverable I/O error occurred on file"
        {
            #corruption
            return (-1,1,11);
        }

        "Exclusive access could not be obtained because the database is in use"
        {
            #this can happen on occassion if a database is stuck restoring and is not able to resume.
            #since we have run a command to make sure we have exclusive access, go ahead and just reseed it
            return (-1,1,11);
        }

        "Reissue the RESTORE statement using WITH REPLACE to overwrite pre-existing files"
        {
            #this is up to the user to specify with a parameter
            return (-1,1);
        }

        "The file .*? was not fully restored by a database or file restore."
        {
            #this can happen if the VSS volume goes offline, log the error and continue on
            return (-1,1)
        }

        "VDI OpenDevice Failed.  HRESULT: 80770004"
        {
            #this can happen if the database is stuck in some weird restoring mode and the VDI cannot even open
            #switch to full backup
            return (-1,1,11);
        }

        #Generic Statements in Errors

        "If possible, restore from backup"
        {
            #corruption
            return (-1,1,11);
        }

        "Restore from a backup of the database"
        {
            #corruption
            return (-1,1,11);
        }

        "An error occurred during recovery"
        {
            #corruption
            return (-1,1,11);
        }

        "A system assertion check has failed"
        {
            #corruption? SQL gone offline?
            #TODO: should this be a reseed?
            #return (-1,1,11);
            return (-1,1);
        }

        "error condition that threatens database integrity and must be corrected immediately"
        {
            #this should cover most page errors, switch to full backup
            return (-1,1,11);
        }

        default
        {
            #some other error we haven't seen before, log it
            return (-1,1);
        }
    }
}

#delete databases that have not been backed up in a while... you should probably only run this on the backup server...
function PurgeOldDatabases
{
    param
    (
        [Parameter(Mandatory=$true)][string]$SQLServer,
        [Parameter(Mandatory=$true)][int]$daysWithoutBackups
    )

    [System.Data.SqlClient.SqlConnection]$DB = $null;
    [System.Data.SqlClient.SqlCommand]$CMD = $null;
    try
    {
        if($daysWithoutBackups -lt 5)
        {
            Write-Verbose "::PurgeOldDatabases - changing daysWithoutBackups from $($daysWithoutBackups) to 5";
            $daysWithoutBackups = 5;
        }

        #Write-Information "$($SQLServer): Purging unused databases with no backup/restore activity for $daysWithoutBackups days...";
        $DB = [System.Data.SqlClient.SqlConnection]::new("Server=$($SQLServer);Database=master;Trusted_Connection=true");
        $DB.Open();
        $CMD = [System.Data.SqlClient.SqlCommand]::new("IF NOT EXISTS(SELECT *
 FROM MSDB.sys.indexes
 WHERE name='DBNameDate' AND object_id = OBJECT_ID('msdb.dbo.backupset'))
BEGIN
CREATE INDEX DBNameDate ON MSDB.dbo.backupset(database_name,backup_finish_date DESC)
END

declare @DBName varchar(4000)
declare @DBSql nvarchar(4000)
declare @CursorStatement nvarchar(4000)

SET @CursorStatement = 'declare dbcur cursor for
 SELECT name FROM sys.databases WHERE name NOT IN (''tempdb'',''master'',''model'',''msdb'') AND name IN
   (SELECT database_name FROM msdb..backupset WHERE type IN (''D'',''I'',''L'') GROUP BY database_name) AND name NOT IN
   (SELECT database_name FROM msdb..backupset WHERE type IN (''D'',''I'',''L'') GROUP BY database_name HAVING MAX(backup_finish_date) >= DATEADD(dd,-$($daysWithoutBackups),GetDate()))'
exec(@CursorStatement)
open dbcur

fetch next from dbcur into @DBName
while @@fetch_status = 0
begin
 select @DBSql = 'DROP DATABASE [' + @DBName + ']' + char(10)
 exec(@DBSql)

 fetch next from dbcur into @DBName
end
close dbcur
deallocate dbcur", $DB);
        $null = $CMD.ExecuteNonQuery();
        $CMD.Dispose();
        $DB.Close();
    }
    catch
    {
        WriteErrorEx $_;
    }
    finally
    {
        if($DB -ne $null)
        {
            $DB.Dispose();
            $DB = $null;
        }
    }
}

#delete DAT files older than days in all subfolders under root path... be cautious of this function
function DeleteOldFiles
{
    param
    (
        [Parameter(Mandatory=$true)][string]$rootPath,
        [Parameter(Mandatory=$true)][int]$daysOld
    )

    try
    {
        if($rootPath.Length -lt 5 -or $daysOld -lt 1) { return; }
        $compareDate = [datetime]::Now.AddDays(-$daysOld);
        Write-Verbose "$($source.paths[0].path): Deleting files under $($rootPath) older than $($daysOld) days ($($compareDate))...";

        $delFiles = Get-ChildItem "$($rootPath)\*.dat*" -File -Recurse | Where-Object LastWriteTime -lt $compareDate;
        Write-Information "Removing $($delFiles.Length) file(s)";
        $delFiles.ForEach({ $null = Remove-Item $_.FullName 2> $null; });

        $delEmptyDirs = Get-ChildItem "$($rootPath)" -Directory -Recurse | Where-Object { (Get-ChildItem $_.FullName).Count -eq 0 };
        Write-Information "Removing $($delEmptyDirs.Length) empty folder(s)";
        $delEmptyDirs.ForEach({ $null = Remove-Item $_.FullName 2> $null; });
    }
    catch
    {
        WriteErrorEx $_;
    }
    finally
    {
    }
}

#Purge Backup History... (see function name)
function PurgeBackupHistory
{
    param
    (
        [Parameter(Mandatory=$true)][string]$SQLServer,
        [Parameter(Mandatory=$true)][int]$keepHistoryDays
    )

    [System.Data.SqlClient.SqlConnection]$DB = $null;
    [System.Data.SqlClient.SqlCommand]$CMD = $null;
    try
    {
        $DB = [System.Data.SqlClient.SqlConnection]::new("Server=$($SQLServer);Database=master;Trusted_Connection=true");
        $DB.Open();
        $sqlCmd = "declare @cutoff datetime
set @cutoff=DATEADD(dd,-$($keepHistoryDays),GetDate())
exec msdb..sp_delete_backuphistory @cutoff";
        Write-Verbose "$($SQLServer): Purging backup history $($keepHistoryDays) days -> $($sqlCmd)";
        $CMD = [System.Data.SqlClient.SqlCommand]::new($sqlCmd, $DB);
        $null = $CMD.ExecuteNonQuery();
        $CMD.Dispose();
        $DB.Close();
    }
    catch
    {
        WriteErrorEx $_;
    }
    finally
    {
        if($DB -ne $null)
        {
            $DB.Dispose();
            $DB = $null;
        }
    }
}

#regular expression search/replace in a file
function RegExReplace
{
    param
    (
        [Parameter(Mandatory=$true)][string]$filePath,
        [Parameter(Mandatory=$true)][string]$regexSearchPattern,
        [Parameter(Mandatory=$true)][string]$regexReplace,
        [Parameter()][bool]$unicode = $false
    )

    try
    {
        if($unicode)
        {
            $regexSearchPattern = [Text.RegularExpressions.RegEx]::Replace($regexSearchPattern, '[^\\]|\\(?!\\)', '$0[\x00]', [Text.RegularExpressions.RegexOptions]::SingleLine);
            $regexReplace = [Text.RegularExpressions.RegEx]::Replace($regexReplace, '[^`]', "`$0`0", [Text.RegularExpressions.RegexOptions]::SingleLine);
        }

        Write-Verbose "$($source.paths[0].path): Running file Search/Replace:`r`n file=$($filePath)`r`n search=$($search)`r`n replace=$($regexReplace)";

        $enc = [Text.Encoding]::GetEncoding(1252);

        $matchOffset = 0;
        $streamChunkSize = 1024 * 1024 * 10;
        $buffer = [System.Text.StringBuilder]::new($streamChunkSize * 2);
        [System.Text.RegularExpressions.Match]$m = $null;

        $reg = [System.Text.RegularExpressions.Regex]::new($regexSearchPattern, [Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [Text.RegularExpressions.RegexOptions]::SingleLine -bor [Text.RegularExpressions.RegexOptions]::Compiled);

        [System.IO.FileStream]$streamFile = [IO.File]::OpenRead($filePath);
        $streamFile.Position = $streamFile.Length;
        while($streamFile.Position -gt 0)
        {
            $readBytes = $streamChunkSize;
            if([long]$readBytes -gt $streamFile.Position)
            {
                $readBytes = [int]$streamFile.Position;
            }

            if($buffer.Length -gt 0)
            {
                #remove all of the previous buffer, starting from the beginning, but leaving the regex search pattern length * 4, in case a search pattern overlaps
                #if we find there are too many repeating patterns in the regex, this multiplier might have to be raised higher
                $removeStart = $regexSearchPattern.Length * 4;
                $removeSize = $buffer.Length - $removeStart;
                if($removeSize -lt 0)
                {
                    $removeStart = 0;
                    $removeSize = $buffer.Length;
                }
                $null = $buffer.Remove($removeStart, $removeSize);

                #keep track of the amount of bytes removed, so we can add it back to the match index below
                $matchOffset += $removeSize;
            }
            $streamBytes = [byte[]]::new($readBytes);
            $streamFile.Position -= $readBytes;
            $null = $streamFile.Read($streamBytes, 0, $readBytes);
            $streamFile.Position -= $readBytes;
            $null = $buffer.Insert(0, $enc.GetString($streamBytes));
            $streamBytes = $null;

            # look for pattern
            #$m = [Text.RegularExpressions.RegEx]::Match($buffer.ToString(), $regexSearchPattern, [Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [Text.RegularExpressions.RegexOptions]::SingleLine);
            $m = $reg.Match($buffer.ToString());
            if($m.Success)
            {
                #if we find a match break out
                $matchOffset = $streamFile.Position + $m.Index;
                break;
            }
        }
        $streamFile.Close()
        #ReadAllBytes($filePath);

        if($m -ne $null -and $m.Success)
        {
            #make sure to add the matchOffset and Index together
            Write-Verbose "$($source.paths[0].path): Found and replaced pattern at file position $($matchOffset)";

            # open to write
            $streamFile = [IO.File]::Open($filePath, [IO.FileMode]::Open, [IO.FileAccess]::Write, [IO.FileShare]::ReadWrite);
            $binaryWriter = [IO.BinaryWriter]::new($streamFile);

            # set file position to location of the string
            $binaryWriter.BaseStream.Position = $matchOffset;
            $replacementBytes = $enc.GetBytes($regexReplace);
            $binaryWriter.Write($replacementBytes);
            $streamFile.Close();
            $streamFile.Dispose();
        }
        else
        {
            Write-Verbose "$($source.paths[0].path): Pattern was not found";
        }
    }
    catch
    {
        WriteErrorEx $_;
    }
}

#create snapshot
function SnapshotCreate
{
    [OutputType([Alphaleonis.Win32.Vss.VssSnapshotProperties])]
    param
    (
        [Parameter(Mandatory=$true)][string]$volume,
        [Parameter()][Alphaleonis.Win32.Vss.VssSnapshotContext]$context = [Alphaleonis.Win32.Vss.VssSnapshotContext]::NasRollback,
        [Parameter()][string]$exposeMountPath = $null
    )

    [Alphaleonis.Win32.Vss.IVssBackupComponents]$c = $null;
    try
    {
        if($volume[$volume.Length - 1] -ne "\") { $volume += "\"; }

        $vss = [Alphaleonis.Win32.Vss.VssUtils]::LoadImplementation();
        $c = $vss.CreateVssBackupComponents();

        $g = [Guid]::Empty;
        $c.InitializeForBackup([NullString]::Value);
        $c.SetContext($context);
        $null = $c.StartSnapshotSet();
        $g = $c.AddToSnapshotSet($volume);
        $c.DoSnapshotSet();
        $ret = $c.GetSnapshotProperties($g);
        if(![string]::IsNullOrEmpty($exposeMountPath)) { $ret = SnapshotExpose $ret $exposeMountPath; }
        return $ret;
    }
    finally
    {
        $c.Dispose();
    }
}

#expose snapshot
function SnapshotExpose
{
    [OutputType([Alphaleonis.Win32.Vss.VssSnapshotProperties])]
    param
    (
        [Parameter(Mandatory=$true)][Alphaleonis.Win32.Vss.VssSnapshotProperties]$snapshot,
        [Parameter(Mandatory=$true)][string]$exposeMountPath
    )

    [Alphaleonis.Win32.Vss.IVssBackupComponents]$c = $null;
    try
    {
        if($exposeMountPath[$exposeMountPath.Length - 1] -ne "\") { $exposeMountPath += "\"; }

        $vss = [Alphaleonis.Win32.Vss.VssUtils]::LoadImplementation();
        $c = $vss.CreateVssBackupComponents();
        $c.InitializeForBackup([NullString]::Value);
        $c.SetContext([Alphaleonis.Win32.Vss.VssSnapshotContext]::All);

        if($exposeMountPath.Length -gt 3)
        {
            try
            {
                #autocreate mountpoint folder if it doesn't exist (don't throw an error if it already does)
                $null = New-Item $exposeMountPath -ItemType Directory;
            }
            catch
            {
            }
        }

        $null = $c.ExposeSnapshot($snapshot.SnapshotId, [NullString]::Value, [Alphaleonis.Win32.Vss.VssVolumeSnapshotAttributes]::ExposedLocally, $exposeMountPath);

        return $c.GetSnapshotProperties($snapshot.SnapshotId);
    }
    finally
    {
        $c.Dispose();
    }
}

#unexpose snapshot
function SnapshotUnexpose
{
    [OutputType([Alphaleonis.Win32.Vss.VssSnapshotProperties])]
    param
    (
        [Parameter(Mandatory=$true)][Alphaleonis.Win32.Vss.VssSnapshotProperties]$snapshot
    )

    [Alphaleonis.Win32.Vss.IVssBackupComponents]$c = $null;
    try
    {
        $vss = [Alphaleonis.Win32.Vss.VssUtils]::LoadImplementation();
        $c = $vss.CreateVssBackupComponents();

        $c.InitializeForBackup([NullString]::Value);
        $c.SetContext([Alphaleonis.Win32.Vss.VssSnapshotContext]::All);

        $snapshot = $c.GetSnapshotProperties($snapshot.SnapshotId);
        $exposeMountPath = $snapshot.ExposedName;
        if(![string]::IsNullOrEmpty($exposeMountPath)) { $c.UnexposeSnapshot($snapshot.SnapshotId); }
        $snapshot = $c.GetSnapshotProperties($snapshot.SnapshotId);
        if($exposeMountPath.Length -gt 3)
        {
            try
            {
                if((Get-ChildItem $exposeMountPath -Attributes Hidden,ReadOnly,System,Directory,Normal).Count -eq 0)
                {
                    $null = Remove-Item $exposeMountPath; #autoremove mountpoint if the folder is empty
                }
            }
            catch
            {
            }
        }
        return $snapshot;
    }
    catch
    {
        WriteErrorEx $_;
        return $null;
    }
    finally
    {
        $c.Dispose();
    }
}

#delete snapshot
function SnapshotDelete
{
    [OutputType([int])]
    [CmdletBinding()]
    param
    (
        [Parameter(Mandatory=$true,ParameterSetName="snapshot",Position=0,ValueFromPipeline)][Alphaleonis.Win32.Vss.VssSnapshotProperties]$snapshot,
        [Parameter(Mandatory=$true,ParameterSetName="volume",Position=0)][string]$volume,
        [Parameter(Mandatory=$true,ParameterSetName="volume",Position=1)][int]$deleteAfterDays
    )

    [Alphaleonis.Win32.Vss.IVssBackupComponents]$c = $null;
    $count = 0;
    try
    {
        $vss = [Alphaleonis.Win32.Vss.VssUtils]::LoadImplementation();
        $c = $vss.CreateVssBackupComponents();

        $c.InitializeForBackup([NullString]::Value);
        $c.SetContext([Alphaleonis.Win32.Vss.VssSnapshotContext]::All);

        if($PSCmdlet.ParameterSetName -eq "snapshot")
        {
            try
            {
                $null = SnapshotUnexpose $snapshot;
                $c.DeleteSnapshot($snapshot.SnapshotId, $true);
                $count++; #no error? it worked
            }
            catch
            {
            }
        }
        else
        {
            if($volume[$volume.Length - 1] -ne "\") { $volume += "\"; }

            $snaps = SnapshotList $volume;
            [Alphaleonis.Win32.Vss.VssSnapshotProperties]$snap = $null;
            foreach($snap in $snaps)
            {
                #delete snapshots older than deleteAfterDays
                if($snap.CreationTimestamp -lt $shared.startTime.AddDays(-$deleteAfterDays))
                {
                    try
                    {
			$null = SnapshotUnexpose $snap;
                        $c.DeleteSnapshot($snap.SnapshotId, $true);
                        $count++; #no error? it worked
                    }
                    catch
                    {
                    }
                }
            }
        }
        return $count;
    }
    catch
    {
        WriteErrorEx $_;
        return $count;
    }
    finally
    {
        $c.Dispose();
    }
}

#list snapshots
function SnapshotList
{
    [OutputType([Collections.Generic.List[Alphaleonis.Win32.Vss.VssSnapshotProperties]])]
    param
    (
        [Parameter()][string]$volume
    )

    [Alphaleonis.Win32.Vss.IVssBackupComponents]$c = $null;
    $l = [Collections.Generic.List[Alphaleonis.Win32.Vss.VssSnapshotProperties]]::new();
    try
    {
        [string]$guidVolume = $null;
        if(![string]::IsNullOrEmpty($volume))
        {
            if($volume[$volume.Length - 1] -ne "\") { $volume += "\"; }
            $guidVolume = GetVolumeGuidFromVolumeMountpoint $volume;
        }

        $vss = [Alphaleonis.Win32.Vss.VssUtils]::LoadImplementation();
        $c = $vss.CreateVssBackupComponents();

        $c.InitializeForBackup([NullString]::Value);
        $c.SetContext([Alphaleonis.Win32.Vss.VssSnapshotContext]::All);
        $snaps = $c.QuerySnapshots();
        [Alphaleonis.Win32.Vss.VssSnapshotProperties]$snap = $null;
        foreach($snap in $snaps)
        {
            if([string]::IsNullOrEmpty($guidVolume) -or $snap.OriginalVolumeName -eq $guidVolume)
            {
                $l.Add($snap);
            }
        }
        return $l;
    }
    catch
    {
        WriteErrorEx $_;
        return $l;
    }
    finally
    {
        $c.Dispose();
    }
}

#microsoft will, by default, delete snapshots if the IO gets too high
#to change this, we need to set the volume protection level to protect the snapshot, not the original volume IO
function SetVolumeProtectionLevel
{
    [OutputType([Alphaleonis.Win32.Vss.VssVolumeProtectionInfo])]
    param
    (
        [Parameter(Mandatory=$true)][string]$volume,
        [Parameter()][Alphaleonis.Win32.Vss.VssProtectionLevel]$protectionLevel = [Alphaleonis.Win32.Vss.VssProtectionLevel]::Snapshot
    )

    [Alphaleonis.Win32.Vss.IVssSnapshotManagement]$m = $null;
    [Alphaleonis.Win32.Vss.IVssDifferentialSoftwareSnapshotManagement]$d = $null;
    try
    {
        if($volume[$volume.Length - 1] -ne "\") { $volume += "\"; }

        $vss = [Alphaleonis.Win32.Vss.VssUtils]::LoadImplementation();
        $m = $vss.GetSnapshotManagementInterface();
        $d = $m.GetDifferentialSoftwareSnapshotManagementInterface();
        $d.SetVolumeProtectionLevel($volume, $protectionLevel);
        return $d.GetVolumeProtectionLevel($volume);
    }
    catch
    {
        WriteErrorEx $_;
    }
    finally
    {
        if($d -ne $null) { $d.Dispose(); }
        if($m -ne $null) { $m.Dispose(); }
    }
}

#if the IO's get too high, Microsoft will either delete your snapshot or take your entire volume offline (depending on the configured protection level)
#this will clear the fault to get the volume back online
function ClearVolumeProtectFault
{
    [OutputType([Alphaleonis.Win32.Vss.VssVolumeProtectionInfo])]
    param
    (
        [Parameter(Mandatory=$true)][string]$volume
    )

    [Alphaleonis.Win32.Vss.IVssSnapshotManagement]$m = $null;
    [Alphaleonis.Win32.Vss.IVssDifferentialSoftwareSnapshotManagement]$d = $null;
    try
    {
        if($volume[$volume.Length - 1] -ne "\") { $volume += "\"; }

        $vss = [Alphaleonis.Win32.Vss.VssUtils]::LoadImplementation();
        $m = $vss.GetSnapshotManagementInterface();
        $d = $m.GetDifferentialSoftwareSnapshotManagementInterface();
        $vpi = $d.GetVolumeProtectionLevel($volume);
        if($vpi.VolumeIsOfflineForProtection)
        {
            $d.ClearVolumeProtectFault($volume);
        }
        return $vpi;
    }
    catch
    {
        WriteErrorEx $_ -addToNotification "ClearVolumeProtectFault($($volume)) -> ";
    }
    finally
    {
        if($d -ne $null) { $d.Dispose(); }
        if($m -ne $null) { $m.Dispose(); }
    }
}

#this will monitor the snapshotstorage freespace every minute, and if it falls below the threshold, it will resize it by creating/deleting a snapshot
#keeping the snapshot storage with ample space prevents Microsoft from deleting your snapshot or taking your entire volume offline (depending on the configured protection level)
function VSSWatchDog
{
    param
    (
        [Parameter()][int]$threshold = 4096 #4GB default threshold
    )

    [Alphaleonis.Win32.Vss.IVssSnapshotManagement]$m = $null;
    [Alphaleonis.Win32.Vss.IVssDifferentialSoftwareSnapshotManagement]$d = $null;
    [int64]$bThreshold = $threshold * 1024L * 1024L;
    try
    {
        Write-Verbose "VSSWatchdog initializing, snapshot space threshold = $($bThreshold.ToString("N0"))";
        $vss = [Alphaleonis.Win32.Vss.VssUtils]::LoadImplementation();
        $m = $vss.GetSnapshotManagementInterface();
        $d = $m.GetDifferentialSoftwareSnapshotManagementInterface();
        $snapshots = SnapshotList;

        $volumes = [Collections.Generic.List[string]]::new();
        foreach($snapshot in $snapshots)
        {
            if(!$volumes.Contains($snapshot.OriginalVolumeName))
            {
                $volumes.Add($snapshot.OriginalVolumeName);
            }
        }

        Write-Verbose "Volumes with snapshots discovered: $($volumes.Count)";
        foreach($volume in $volumes)
        {
            Write-Verbose " - $($volume)";
        }

        #if there aren't any snapshots, exit
        if($volumes.Count -eq 0)
        {
            Write-Verbose "No snapshot volumes detected, VSSWatchdog exiting...";
            return;
        }

        $null = Get-Service; #need to run this first so System.ServiceProcess namespace exists
        [ServiceProcess.ServiceController[]]$sql = $null;
        $services = [Collections.Generic.List[ServiceProcess.ServiceController]]::new();
        try
        {
            $sql = Get-Service "MSSQLSERVER" | Where-Object { $_.Status -eq [ServiceProcess.ServiceControllerStatus]::Running };
        }
        catch
        {
            #ignore error if servicename does not exist
        }

        if($sql -ne $null)
        {
            $services.Add($sql[0]);
        }
        try
        {
            $sql = Get-Service "MSSQL$*" | Where-Object { $_.Status -eq [ServiceProcess.ServiceControllerStatus]::Running };
        }
        catch
        {
            #ignore error if servicename does not exist
        }
        foreach($service in $sql)
        {
            $services.Add($service);
        }

        #remove any services that don't have a default path to a VSS volume snapshot
        for($i=$services.Count-1; $i -ge 0; $i--)
        {
            [string]$dataPath = $null;
            [string]$logPath = $null;
            if($services[$i].Name -eq "MSSQLSERVER")
            {
                $dataPath,$logPath = GetDefaultSQLPaths;
            }
            else
            {
                $dataPath,$logPath = GetDefaultSQLPaths $services[$i].Name.Substring(6);
            }

            if($dataPath.Length -ge 3)
            {
                $dataPath = GetVolumeGuidFromVolumeMountpoint $dataPath.Substring(0, 3);
                if($dataPath -in $volumes)
                {
                    continue;
                }
            }
            if($logPath.Length -ge 3)
            {
                $logPath = GetVolumeGuidFromVolumeMountpoint $logPath.Substring(0, 3);
                if($logPath -in $volumes)
                {
                    continue;
                }
            }
            $null = $services.RemoveAt($i);
        }

        Write-Verbose "Running SQL services discovered: $($services.Count)";
        foreach($service in $services)
        {
            Write-Verbose " - $($service.Name)";
        }

        while($true)
        {
            foreach($volume in $volumes)
            {
                try
                {
                    #double check that we haven't lost the volume
                    $vpl = $d.GetVolumeProtectionLevel($volume);
                    if($vpl.VolumeIsOfflineForProtection)
                    {
                        Write-Verbose "Volume $($volume) is offline, clearing VSS protection fault...";
                        try
                        {
                            #recover from volume protection fault
                            $d.ClearVolumeProtectFault($volume);
                        }
                        catch
                        {
                            WriteErrorEx $_ -addToNotification "VSSWatchdog -> ";
                        }

                        #restart SQL services
                        Write-Verbose "Volume $($volume) should be back online, restarting SQL services...";
                        foreach($service in $services) { try { $service.Stop(); } catch { } }
                        $services.WaitForStatus([ServiceProcess.ServiceControllerStatus]::Stopped, [timespan]::new(0, 5, 0));
                        foreach($service in $services) { try { $service.Start(); } catch { } }
                        $services.WaitForStatus([ServiceProcess.ServiceControllerStatus]::Running, [timespan]::new(0, 5, 0));
                        Write-Verbose "SQL services done restarting";
                    }
                    else
                    {
                        #Write-Verbose "Volume $($volume) is not offline";
                    }

                    $sizes = $d.QueryDiffAreasForVolume($volume);
                    if($sizes.Count -gt 0)
                    {
                        Write-Verbose "Volume $($volume) diff space = $(($sizes[0].AllocatedDiffSpace - $sizes[0].UsedDiffSpace).ToString("N0"))";
                        if($sizes[0].AllocatedDiffSpace - $sizes[0].UsedDiffSpace -le $bThreshold)
                        {
                            #resize shadowstorage diff area by creating and deleting a snapshot
                            Write-Verbose "Threshold hit, creating snapshot";

                            #create a mutex to signal that we are expanding the diff area, and other threads should wait before starting a new restore
                            [bool]$wasCreated = $false;
                            $mutex = [Threading.Mutex]::new($true, "Global\PowerBakVSS", [ref]$wasCreated);

                            if(!$wasCreated)
                            {
                                try
                                {
                                    $null = $mutex.WaitOne();
                                }
                                catch
                                {
                                    #should never have an error, but just in case, catch it so we do not leave the mutex unreleased
                                    WriteErrorEx $_;
                                }
                            }

                            try
                            {
                                [Alphaleonis.Win32.Vss.IVssBackupComponents]$c = $null;
                                $c = $vss.CreateVssBackupComponents();

                                #create snapshot
                                $c.InitializeForBackup([NullString]::Value);
                                $c.SetContext([Alphaleonis.Win32.Vss.VssSnapshotContext]::FileShareBackup);
                                $null = $c.StartSnapshotSet();
                                $null = $c.AddToSnapshotSet($volume);
                                $c.DoSnapshotSet();

                                Write-Verbose "Deleting snapshot";
                                #delete snapshot
                                $c.AbortBackup();

                                #getting the diff area size after a snapshot can cause a bit of disk activity, so run this twice before we release the mutex
                                $sizes = $d.QueryDiffAreasForVolume($volume);
                                $null = $sizes[0].AllocatedDiffSpace;
                                $null = $sizes[0].UsedDiffSpace;

                                Start-Sleep -Milliseconds 5000;

                                $sizes = $d.QueryDiffAreasForVolume($volume);
                                $null = $sizes[0].AllocatedDiffSpace;
                                $null = $sizes[0].UsedDiffSpace;
                            }
                            finally
                            {
                                $c.Dispose();

                                $mutex.ReleaseMutex();
                                $mutex.Close();
                                $mutex.Dispose();
                                $mutex = $null;
                            }
                        }
                    }
                }
                catch
                {
                    WriteErrorEx $_ -addToNotification "VSSWatchdog -> ";
                }
            }

            #wait 10 seconds between shadow storage checks
            Start-Sleep -Milliseconds 10000;
        }
    }
    catch
    {
        WriteErrorEx $_ -addToNotification "VSSWatchDog -> ";
    }
    finally
    {
        if($d -ne $null) { $d.Dispose(); }
        if($m -ne $null) { $m.Dispose(); }
    }
}

#convert "C:\" to "\\?\Volume{00000000-0000-0000-0000-000000000000}\", needed for VSS functions
function GetVolumeGuidFromVolumeMountpoint
{
    [OutputType([string])]
    param
    (
        [Parameter(Mandatory=$true)][string]$volume
    )

    $sig = '[DllImport("kernel32.dll")]
public static extern bool GetVolumeNameForVolumeMountPoint(string lpszVolumeMountPoint, System.Text.StringBuilder lpszVolumeName, int cchBufferLength);';

    $kernel32 = Add-Type -MemberDefinition $sig -Name PInvoke -Namespace GetVolumeNameForVolumeMountPoint -PassThru;

    if($volume[$volume.Length - 1] -ne "\") { $volume += "\"; }

    $guidVolume = [Text.StringBuilder]::new(50);
    if($kernel32::GetVolumeNameForVolumeMountPoint($volume, $guidVolume, $guidVolume.Capacity) -eq $true)
    {
        return $guidVolume.ToString();
    }
    return $null;
}

#check to see if the VSS watchdog needs us to pause while it expands the VSS differential area
function CheckVSSWatchDog()
{
    param
    (
    )

    [Threading.Mutex]$mutex = $null;
    if([Threading.Mutex]::TryOpenExisting("Global\PowerBakVSS", [ref]$mutex))
    {
        #if VSS is performing an action that wants us to wait, then here's where we'll sit
        try
        {
            $null = $mutex.waitOne();
        }
        catch
        {
            #should never be an error, but just in case, do not leave the mutex unreleased
            WriteErrorEx $_;
        }
        finally
        {
            $mutex.ReleaseMutex();
            $mutex.Close();
            $mutex.Dispose();
            $mutex = $null;
        }
    }
}

#check to see if we need to stop processing (this runs on the destination)
function CheckForStopProcessing()
{
    [OutputType([bool])]
    param
    (
    )

    [Threading.Mutex]$mutex = $null;
    if([Threading.Mutex]::TryOpenExisting($shared.gracefulStopMutexName, [ref]$mutex))
    {
        #if we were requested to halt, this is where we will end up
        $mutex.Close();
        $mutex.Dispose();
        $mutex = $null;

        return $true;
    }
    return $false;
}

#function to easily add/subtract from full/differential/incremental backup
function AddMetric
{
    param
    (
        [Parameter(Mandatory=$true)][int]$backupType,
        [Parameter()][int]$addCount = 1
    )

    switch($backupType)
    {
        0 { $shared.metrics.Full += $addCount; }
        1 { $shared.metrics.Differential += $addCount; }
        2 { $shared.metrics.Incremental += $addCount; }
    }
}

#register scheduled task (in case you couldn't read the function name)
function RegisterScheduledTask
{
    param
    (
        [Parameter(Mandatory=$true)][string]$TaskName,
        [Parameter(Mandatory=$true)][datetime]$StartTime,
        [Parameter()][UInt32]$DayInterval = 1,
        [Parameter(Mandatory=$true)][string]$LogonId,
        [Parameter()][Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.LogonTypeEnum]$LogonType = [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.LogonTypeEnum]::Password,
        [Parameter()][Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.RunLevelEnum]$RunLevel = [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.RunLevelEnum]::Limited,

        [Parameter(Mandatory=$true)][hashtable]$Parameters
    )

    #make sure \PowerBak folder exists in Scheduled Tasks
    $scheduleObject = New-Object -ComObject schedule.service;
    $scheduleObject.Connect();
    $rootFolder = $scheduleObject.GetFolder("\");
    try
    {
        $null = $rootFolder.GetFolder("PowerBak");
    }
    catch
    {
        $null = $rootFolder.CreateFolder("PowerBak");
    }
    [Runtime.InteropServices.Marshal]::FinalReleaseComObject($scheduleObject);

    #turn parameters into a string we can use
    $pList = [System.Collections.Generic.List[string]]::new();
    $Parameters.Keys.foreach({
        if($_ -ne "ScheduledTask") #skip ScheduledTask parameter (so we don't go into a recursive loop creating the scheduled task over and over)
        {
            $pList.Add("-$($_)");
            switch($Parameters[$_])
            {
                { $_ -match "\s" }
                {
                    $pList.Add("\`"$($_)\`"");
                    break;
                }

                "True"
                {
                    #$pList.Add("`$true");
                    break;
                }

                "False"
                {
                    $pList.Add("`$false");
                    break;
                }

                default
                {
                    $pList.Add($_);
                    break;
                }
            }
        }
    });
    $sParameters = [string]::Join(" ", $pList);

    #create scheduled task definition, composed of a Trigger, Action, Principal, and Settings
    $Trigger = New-ScheduledTaskTrigger -At $StartTime -Daily -DaysInterval $DayInterval;
    #fix trigger so it doesn't "sync across timezones", which basically makes it ignore Daylight Savings Time
    $Trigger.StartBoundary = [DateTime]::Parse($Trigger.StartBoundary).ToLocalTime().ToString("s")

    $Action = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-Command `"&{.\PowerBak.ps1 $($sParameters)}`"" -WorkingDirectory $PSScriptRoot;
    [CimInstance]$Principal = $null;
    switch($LogonType)
    {
        { $_ -in [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.LogonTypeEnum]::Group }
        {
            $Principal = New-ScheduledTaskPrincipal -GroupId $LogonId -RunLevel $RunLevel;
        }

        { $_ -in [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.LogonTypeEnum]::ServiceAccount,[Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.LogonTypeEnum]::S4U,[Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.LogonTypeEnum]::None }
        {
            $Principal = New-ScheduledTaskPrincipal -UserId $LogonId -LogonType $LogonType -RunLevel $RunLevel;
        }
    }

    $Settings = New-ScheduledTaskSettingsSet -Compatibility Win8 -MultipleInstances IgnoreNew -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit ([timespan]::new(0));
    if($Principal -eq $null)
    {
        $sp = Read-Host "Password" -AsSecureString;
        $cred = [System.Management.Automation.PSCredential]::new($LogonId, $sp);
        $p = $cred.GetNetworkCredential().Password;
        Register-ScheduledTask -TaskName $TaskName -TaskPath "\PowerBak\" -Action $Action -Trigger $Trigger -Settings $Settings -User $LogonId -Password $p -RunLevel $RunLevel -Force;
    }
    else
    {
        Register-ScheduledTask -TaskName $TaskName -TaskPath "\PowerBak\" -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Force;
    }
}

#this will reflect a basic class definition that has the properties (no constructors/methods), for use in re-creating in the PSSession
function getClassDef
{
    [OutputType([string])]
    param
    (
        [Parameter(Mandatory=$true)][Type]$classType
    )

    $create = "class $($classType.Name)`r`n{`r`n ";
    $create += $classType.GetProperties() | ForEach-Object { "   [$($_.PropertyType.ToString())]`$$($_.Name)`r`n"; }
    $create += "}";

    return $create;
}

#called from the conducting server to create remote sessions on both the source and destination servers
function InitRemoteSession
{
    [OutputType([Management.Automation.Runspaces.PSSession])]
    param
    (
        [Parameter(Mandatory=$true)][string]$server,
        [Parameter(Mandatory=$true)][SyncJob]$jobInfo,
        [Parameter()][PSCredential]$credentials
    )

    try
    {
        $jobInfo.shared.whoami = $server;
        $err = $null;
        [Management.Automation.Runspaces.PSSession]$remote = $null;
        $option = New-PSSessionOption -CancelTimeout 2000 -IdleTimeout $shared.networkTimeout -OutputBufferingMode Block; # -NoCompression -OperationTimeout 0;

        #remove port number if it exists (right of comma, i.e. 1.2.3.4, 1433)
        $commaLoc = $server.IndexOf(',');
        if($commaLoc -ge 0) { $server = $server.Substring(0, $commaLoc); }

        if($credentials -eq $null)
        {
            $remote = New-PSSession $server -SessionOption $option -ErrorAction SilentlyContinue -ErrorVariable err;
        }
        else
        {
            $remote = New-PSSession $server -SessionOption $option -ErrorAction SilentlyContinue -ErrorVariable err -Credential $credentials;
        }
        if($remote -eq $null)
        {
            if($err.Exception.Message -match "HTTPS or the destination is in the TrustedHosts list")
            {
                [Microsoft.WSMan.Management.WSManConfigLeafElement]$trustedHosts = Get-Item "WSMan:\localhost\Client\TrustedHosts";
                if($trustedHosts.Value -notmatch "(^|\D)$($server.Replace(".", "\."))($|\D)")
                {
                    $confirmation = Read-Host "$($err.Exception.Message)`r`n`r`nThe server $($server) needs to be in the TrustedHosts list. Would you like to automatically add this?";
                    if($confirmation -eq "y")
                    {
                        $newTrustedHosts = $server;
                        if(![string]::IsNullOrEmpty($trustedHosts.Value))
                        {
                            $newTrustedHosts = "$($trustedHosts.Value),$($newTrustedHosts)";
                        }
                        Set-Item "WSMan:\localhost\Client\TrustedHosts" $newTrustedHosts -Force;

                        #try again after adding trusted host
                        $remote = InitRemoteSession $server $jobInfo $credentials;
                        return $remote;
                    }
                }
                else
                {
                    Write-Host "$($err.Exception.Message)`r`n`r`nThe server $($server) is in the TrustedHosts list. Did you forget credentials?";
                }
            }
            if($err.Exception.Message -match "Access is denied") {
                throw "You may need elevated privileges to connect to the server: $($server)`r`n  $($err.Exception.Message)";
            }
            throw $err;
        }

        #class definitions have to be copied over to the remote session very carefully, as powershell doesn't support a good mechanism yet
        $classDefs = [string[]]::new(5);
        $classDefs[0] = getClassDef([Metrics]);
        $classDefs[1] = getClassDef([DatabasePath]);
        $classDefs[2] = getClassDef([DatabaseResource]);
        $classDefs[3] = getClassDef([MetaFile]);
        $classDefs[4] = getClassDef([MetaData]);

        #Prepare remote session
        Invoke-Command $remote -ScriptBlock {
            Set-StrictMode -Version 1.0;

            $basePath = $Using:PSScriptRoot;

            Add-Type -Path "$($basePath)\lib\AlphaVSS.Common.dll";
            Add-Type -Path "$($basePath)\lib\VdiDotNet.dll";
            Add-Type -Path "$($basePath)\lib\EchoStream.NET.dll";
            Add-Type -Path "$($basePath)\lib\ZStandard.Net.dll";

            $VerbosePreference = $Using:VerbosePreference;
            #$InformationPreference = [System.Management.Automation.ActionPreference]::Continue;

            Invoke-Expression $Using:function:WriteErrorEx.Ast.Extent.Text;
            Invoke-Expression $Using:function:WriteErrorAgg.Ast.Extent.Text;
            Invoke-Expression $Using:function:GetMetaData.Ast.Extent.Text;
            Invoke-Expression $Using:function:PutMetaData.Ast.Extent.Text;
            Invoke-Expression $Using:function:ExecuteDBCommand.Ast.Extent.Text;
            Invoke-Expression $Using:function:CreateTCPListener.Ast.Extent.Text;
            Invoke-Expression $Using:function:ProcessTCPConnections.Ast.Extent.Text;
            Invoke-Expression $Using:function:ConnectToDestinationServer.Ast.Extent.Text;
            Invoke-Expression $Using:function:CloseObjects.Ast.Extent.Text;
            Invoke-Expression $Using:function:SetKeepAlive.Ast.Extent.Text;
            Invoke-Expression $Using:function:WaitOOBCommand.Ast.Extent.Text;
            Invoke-Expression $Using:function:GetOOBCommand.Ast.Extent.Text;
            Invoke-Expression $Using:function:SendOOBCommand.Ast.Extent.Text;
            Invoke-Expression $Using:function:CopyStream.Ast.Extent.Text;
            Invoke-Expression $Using:function:OOBCopyStreamResult.Ast.Extent.Text;
            Invoke-Expression $Using:function:EstimateFileSize.Ast.Extent.Text;
            Invoke-Expression $Using:function:GetDefaultSQLPaths.Ast.Extent.Text;
            Invoke-Expression $Using:function:ExpandDatabases.Ast.Extent.Text;
            Invoke-Expression $Using:function:ExpandFilenames.Ast.Extent.Text;
            Invoke-Expression $Using:function:StreamSQLCommand.Ast.Extent.Text;
            Invoke-Expression $Using:function:ReceiveFile.Ast.Extent.Text;
            Invoke-Expression $Using:function:ReceiveSQLStream.Ast.Extent.Text;
            Invoke-Expression $Using:function:SendFile.Ast.Extent.Text;
            Invoke-Expression $Using:function:AutoRestoreDatabase.Ast.Extent.Text;
            Invoke-Expression $Using:function:GenerateStandbyUndoFilename.Ast.Extent.Text;
            Invoke-Expression $Using:function:BackupDatabase.Ast.Extent.Text;
            Invoke-Expression $Using:function:RestoreDatabase.Ast.Extent.Text;
            Invoke-Expression $Using:function:CheckDBError.Ast.Extent.Text;
            Invoke-Expression $Using:function:PurgeOldDatabases.Ast.Extent.Text;
            Invoke-Expression $Using:function:DeleteOldFiles.Ast.Extent.Text;
            Invoke-Expression $Using:function:PurgeBackupHistory.Ast.Extent.Text;
            Invoke-Expression $Using:function:RegExReplace.Ast.Extent.Text;
            Invoke-Expression $Using:function:SnapshotCreate.Ast.Extent.Text;
            Invoke-Expression $Using:function:SnapshotExpose.Ast.Extent.Text;
            Invoke-Expression $Using:function:SnapshotUnexpose.Ast.Extent.Text;
            Invoke-Expression $Using:function:SnapshotDelete.Ast.Extent.Text;
            Invoke-Expression $Using:function:SnapshotList.Ast.Extent.Text;
            Invoke-Expression $Using:function:SetVolumeProtectionLevel.Ast.Extent.Text;
            Invoke-Expression $Using:function:ClearVolumeProtectFault.Ast.Extent.Text;
            Invoke-Expression $Using:function:VSSWatchDog.Ast.Extent.Text;
            Invoke-Expression $Using:function:GetVolumeGuidFromVolumeMountpoint.Ast.Extent.Text;
            Invoke-Expression $Using:function:CheckVSSWatchDog.Ast.Extent.Text;
            Invoke-Expression $Using:function:AddMetric.Ast.Extent.Text;
            Invoke-Expression $Using:function:CheckForStopProcessing.Ast.Extent.Text;

            foreach($classDef in $Using:classDefs) { Invoke-Expression $classDef; }
            $shared = $Using:jobInfo.shared;

            $source = $Using:jobInfo.source;
            $source.paths = [DatabasePath[]]::new($Using:jobInfo.source.paths.Length);
            $source.paths[0] = $Using:jobInfo.source.paths[0];
            #if($source.paths.Length -gt 1) { $source.paths[1] = $Using:source.paths[1]; }

            $dest = $Using:jobInfo.dest;
            $dest.paths = [DatabasePath[]]::new($Using:jobInfo.dest.paths.Length);
            $dest.paths[0] = $Using:jobInfo.dest.paths[0];
            $dest.paths[1] = $Using:jobInfo.dest.paths[1];

            $shared.metrics = [Metrics]::new();
            #Write-Verbose $source.paths | ConvertTo-Json;
            #Write-Verbose $dest.paths | ConvertTo-Json;
        };

        return $remote;
    }
    catch
    {
        throw;
    }

}

#converts a PSCustomObject to a HashTable, needed because you can't Splat a PSCustomObject, only a HashTable
function PSCustomObjectToHashTable
{
    [OutputType([hashtable])]
    param
    (
        [Parameter(Mandatory=$true)][PSCustomObject]$ps
    )

    $ret = @{};
    foreach($p in $ps.PSObject.Properties)
    {
        if($p.Value.GetType() -eq [Management.Automation.PSCustomObject])
        {
            if($p.Value.IsPresent -ne $null)
            {
                $ret[$p.Name] = [switch]::new($p.Value.IsPresent);
            }
            else
            {
                $ret[$p.Name] = PSCustomObjectToHashTable $p.Value;
            }
        }
        else
        {
            $ret[$p.Name] = $p.Value;
        }
    }
    return $ret;
}

#Set up a PSSession between the source and destination servers
#Call StartJob([SyncJob]) to start the backup restore jobs between the source/destination servers
#This functionality was separated so we can control how many jobs run in parallel
#This function is also used to set up the VSS Watchdog Timer job (RunVSSWatchdogSession switch)
function SetupJob
{
    [OutputType([SyncJob])]
    #[CmdletBinding(DefaultParameterSetName="MaintenanceOnly2")]
    param
    (
        [Parameter(ParameterSetName="M",Position=0,Mandatory=$true)][string]$FromServer,
        [Parameter(ParameterSetName="M",Position=1,Mandatory=$true)][string]$FromPath,
        [Parameter(ParameterSetName="M",Position=2)][Parameter(ParameterSetName="MaintenanceOnly1",Mandatory=$true)][Parameter(ParameterSetName="MaintenanceOnly2",Mandatory=$true)][string]$ToServer = "localhost",
        [Parameter(ParameterSetName="M",Position=3,Mandatory=$true)][Parameter(ParameterSetName="MaintenanceOnly1",Mandatory=$true)][string[]]$ToPath,
        [Parameter(ParameterSetName="M",Position=4)][PSCredential]$FromServerCredentials,
        [Parameter(ParameterSetName="M",Position=5)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1")][Parameter(ParameterSetName="MaintenanceOnly2")][PSCredential]$ToServerCredentials,
        [Parameter(ParameterSetName="M",Position=6)][ValidateSet("Full", "Differential", "Incremental")][string]$BackupType = "Incremental",
        [Parameter(ParameterSetName="M",Position=7)][ValidateSet("Recovery", "NoRecovery", "Standby")][string]$RecoveryType = "Standby",
        [Parameter(ParameterSetName="M",Position=8)][switch]$Reverse,
        [Parameter(ParameterSetName="M",Position=9)][switch]$SkipWritingDBProperty,
        [Parameter(ParameterSetName="M",Position=10)][ValidateRange(0, [int]::MaxValue)][int]$SinceHours = 0,
        [Parameter(ParameterSetName="M",Position=11)][ValidateRange(0, [int]::MaxValue)][int]$NetworkTimeout = 60 * 60 * 2,
        [Parameter(ParameterSetName="M",Position=12)][ValidateRange(0, [int]::MaxValue)][int]$BufferSize = 1024 * 1024,
        [Parameter(ParameterSetName="M",Position=13)][ValidateRange(0, 65535)][int]$TCPPort = 0,
        [Parameter(ParameterSetName="M",Position=14)][ValidateRange(0, 22)][int]$CompressionLevel = 6,
        [Parameter(ParameterSetName="M",Position=15)][ValidateScript({ $_ % 65536 -eq 0 -and $_ -le 4194304; })][int]$MaxTransferSize = 65536 * 16,
        [Parameter(ParameterSetName="M",Position=16)][Parameter(ParameterSetName="FileConfig")][ValidateRange(0, [int]::MaxValue)][int]$BufferCount = 100,
        [Parameter(ParameterSetName="M",Position=17)][ValidateSet("Full", "Bulk_Logged")][string]$SetIncrementalLoggingType = "Bulk_Logged",
        [Parameter(ParameterSetName="M",Position=18)][switch]$SkipFilePathOnInitialSeeding,
        [Parameter(ParameterSetName="M",Position=19)][switch]$Replace,
        [Parameter(ParameterSetName="M",Position=20)][switch]$CopyOnly,
        [Parameter(ParameterSetName="M",Position=21)][switch]$ContinueAfterError,
        [Parameter(ParameterSetName="M",Position=22)][Parameter(ParameterSetName="FileConfig")][switch]$ToCheckDB,
        [Parameter(ParameterSetName="M",Position=23)][ValidateSet("OnlySeed", "NoSeed", "AllowSeed")][string]$InitialSeeding = "AllowSeed",
        [Parameter(ParameterSetName="M",Position=24)][switch]$RandomizeOrder,
        [Parameter(ParameterSetName="M",Position=25)][Parameter(ParameterSetName="FileConfig")][switch]$SaveFilesAsBak,
        [Parameter(ParameterSetName="M",Position=26)][Parameter(ParameterSetName="FileConfig")][string]$SQLDataPath,
        [Parameter(ParameterSetName="M",Position=27)][Parameter(ParameterSetName="FileConfig")][string]$SQLLogPath,
        [Parameter(ParameterSetName="M",Position=28)][Parameter(ParameterSetName="FileConfig")][Parameter(ParameterSetName="MaintenanceOnly1")][Parameter(ParameterSetName="MaintenanceOnly2")][switch]$AppendFromServerFolderToPath,
        [Parameter(ParameterSetName="M",Position=29)][Parameter(ParameterSetName="MaintenanceOnly1")][Parameter(ParameterSetName="MaintenanceOnly2")][switch]$AppendDateTimeFolderToPath,
        [Parameter(ParameterSetName="M",Position=30)][Parameter(ParameterSetName="MaintenanceOnly1")][ValidateRange(0, 365 * 10)][int]$PurgeBackupHistoryDays = 0,
        [Parameter(ParameterSetName="M",Position=31)][Parameter(ParameterSetName="MaintenanceOnly1")][ValidateRange(0, 365 * 10)][int]$DeleteAfterNoBackupRestoreDays = 0,
        [Parameter(ParameterSetName="M",Position=32)][Parameter(ParameterSetName="MaintenanceOnly1")][ValidateRange(0, 365 * 10)][int]$DeleteOldFiles = 0,
        [Parameter(ParameterSetName="M",Position=33)][Parameter(ParameterSetName="MaintenanceOnly1")][Parameter(ParameterSetName="MaintenanceOnly2",Mandatory=$true)][hashtable]$SnapshotManagement = @{},

        [Parameter(ParameterSetName="M",Position=34)][DateTime]$AutoRestoreToDateTime = [DateTime]::MinValue,
        [Parameter(ParameterSetName="M",Position=35)][Parameter(ParameterSetName="MaintenanceOnly1")][Parameter(ParameterSetName="VSSWatchdog")][ValidateRange(0, [int64]::MaxValue)][int64]$VSSWatchdog = 0L,

        [Parameter(Position=36)][switch]$EmailNotification,
        [Parameter(Position=37)][switch]$RunVSSWatchdogSession #private
    )

    $jobInfo = [SyncJob]::new();
    try
    {
        #$ss = [initialsessionstate]::CreateDefault();
        #$ss.Commands.Add([Management.Automation.Runspaces.SessionStateFunctionEntry]::new($function:CreateTCPListener.Ast.Name, $function:CreateTCPListener));
        #$ss.Commands.Add([Management.Automation.Runspaces.SessionStateFunctionEntry]::new($function:CopyStream.Ast.Name, $function:CopyStream));
        #$ss.Commands.Add([Management.Automation.Runspaces.SessionStateFunctionEntry]::new($function:BackupDatabase.Ast.Name, $function:BackupDatabase));
        #$ss.Commands.Add([Management.Automation.Runspaces.SessionStateFunctionEntry]::new($function:RestoreDatabase.Ast.Name, $function:RestoreDatabase));
        #$ss.Commands.Add([Management.Automation.Runspaces.SessionStateFunctionEntry]::new($function:StreamSQLCommand.Ast.Name, $function:StreamSQLCommand));
        #$j = [PowerShell]::Create($ss)
        #$j.Runspace.SessionStateProxy.SetVariable("shared", $shared);
        #$null = $j.AddScript(
        #});
        #$invoke = $j.BeginInvoke();

        #variable to hold config to share with remote servers
        $shared = @{};
        $script:shared = $shared;
        $jobInfo.shared = $shared;

        if($AutoRestoreToDateTime -gt [DateTime]::MinValue)
        {
            #autorestore requires some assumed paramters
            $CopyOnly = $false;
            $ContinueAfterError = $true;
            $RecoveryType = "Recovery";
        }

        $shared.backupType = $BackupType;
        $shared.recoveryType = $RecoveryType;
        $shared.reverse = $Reverse;
        $shared.skipWritingDBProperty = $SkipWritingDBProperty;
        $shared.sinceHours = $SinceHours;
        $shared.copyOnly = $CopyOnly;
        $shared.networkTimeout = $NetworkTimeout * 1000;
        $shared.bufferSize = $BufferSize;
        $shared.TCPPort = $TCPPort;
        $shared.compressionLevel = $CompressionLevel;
        $shared.maxTransferSize = $MaxTransferSize;
        $shared.bufferCount = $BufferCount;
        $shared.setIncrementalLoggingType = $SetIncrementalLoggingType;
        $shared.skipFilePathOnInitialSeeding = $SkipFilePathOnInitialSeeding;
        $shared.replace = $Replace;
        $shared.initialSeeding = $InitialSeeding;
        $shared.continueAfterError = $ContinueAfterError;
        $shared.toCheckDB = $ToCheckDB;
        $shared.purgeBackupHistoryDays = $PurgeBackupHistoryDays;
        $shared.deleteAfterNoBackupRestoreDays = $DeleteAfterNoBackupRestoreDays;
        $shared.deleteOldFiles = $DeleteOldFiles;
        $shared.randomizeOrder = $RandomizeOrder;
        $shared.saveFilesAsBak = $SaveFilesAsBak;
        #make sure any SQL overridden paths ends with a backslash \
        if(![string]::IsNullOrEmpty($SQLDataPath) -and $SQLDataPath[$SQLDataPath.Length - 1] -ne "\") { $SQLDataPath += "\"; }
        if(![string]::IsNullOrEmpty($SQLLogPath) -and $SQLLogPath[$SQLLogPath.Length - 1] -ne "\") { $SQLLogPath += "\"; }
        $shared.overrideSQLPaths = @($SQLDataPath,$SQLLogPath);
        $shared.appendFromServerFolderToPath = $AppendFromServerFolderToPath;
        $shared.appendDateTimeFolderToPath = $AppendDateTimeFolderToPath;
        $shared.snapshotManagement = $SnapshotManagement;
        $shared.autoRestoreToDateTime = $AutoRestoreToDateTime;
        $shared.startTime = $StartTime;
        $shared.maintenanceOnly = if($PSCmdlet.ParameterSetName.Length -ge 15 -and $PSCmdlet.ParameterSetName.Substring(0, 15) -eq "MaintenanceOnly") { $true } else { $false };
        $shared.VSSWatchdog = $VSSWatchdog; #renamed here on purpose, so we control exactly which sessions are watchdog

        if($ToServer -eq "localhost")
        {
            #localhost is always 127.0.0.1 or ::1, and the destination server needs a real IP address
            $ToServer = $env:COMPUTERNAME;
        }

        #remove port number if it exists (right of comma, i.e. 1.2.3.4, 1433)
        $ToServerNoPort = $ToServer;
        $commaLoc = $ToServerNoPort.IndexOf(',');
        if($commaLoc -ge 0) { $ToServerNoPort = $ToServerNoPort.Substring(0, $commaLoc); }

        $shared.address = [Net.Dns]::GetHostAddresses($ToServerNoPort) | WHERE IPAddressToString -match "192\.168\." | SELECT -FIRST 1;
        $shared.security = [guid]::NewGuid();
        $shared.gracefulStopMutexName = $script:gracefulStopMutexName;

        ##FURTHER VALIDATE PARAMETERS
        switch($shared.initialSeeding)
        {
            "AllowSeed"
            {
                break;
            }

            default
            {
                if($shared.backupType -eq "Full")
                {
                    Write-Verbose "InitialSeeding options can only be used when using Differential or Incremental (log) Backups";
                    return $null;
                }
                break;
            }
        }

        if($Reverse -eq $true)
        {
            if($AutoRestoreToDateTime -gt [DateTime]::MinValue)
            {
                Write-Error "Cannot combine -Reverse and -AutoRestoreToDateTime switches";
                return $null;
            }
            if($BackupType -ne "Incremental")
            {
                Write-Error "Can only combine -Reverse switch with -BackupType Incremental";
                return $null;
            }
            if($CopyOnly -eq $true)
            {
                Write-Error "Cannot combine -Reverse and -CopyOnly switches";
                return $null;
            }
            if($RecoveryType -eq "Recovery")
            {
                Write-Error "Cannot combine -Reverse and -RecoveryType Recovery, because this would break the backup chain";
                return $null;
            }
        }
        ##END VALIDATION

        #Set up session with SOURCE server
        $source = [DatabaseResource]::new($FromServer, $FromPath, $ToServer, $true, !$shared.maintenanceOnly);
        $jobInfo.source = $source;

        #for($i = 0; $i -lt $ToPath.Count; $i++)
        #{
        #    if($ToPath[$i][$ToPath[$i].Length - 1] -ne "\") { $ToPath[$i] += "\"; }
        #}
        $dest = [DatabaseResource]::new($ToServer, $ToPath, $FromServer, $false, !$shared.maintenanceOnly);
        $jobInfo.dest = $dest;

        #Set up session with DESTINATION server first
        $shared.isDestination = $true;
        $jobInfo.remoteDest = InitRemoteSession $dest.server $jobInfo $ToServerCredentials;

        #if we are only running maintenance or a VSSWatchdog, we don't need to set anything else up
        if($shared.maintenanceOnly -or $RunVSSWatchdogSession) { return $jobInfo; }

        #Set up TCPListener on destination server (retrieve port being used)
        $shared.TCPPort = Invoke-Command $jobInfo.remoteDest -ScriptBlock {
            Set-StrictMode -Version 1.0;

            CreateTCPListener $shared.address $shared.TCPPort;
        };

        #Set up session with SOURCE server second
        $shared.isDestination = $false;
        $jobInfo.remoteSrc = InitRemoteSession $source.server $jobInfo $FromServerCredentials;

        $jobInfo.started = $false;

        $Host.PrivateData.VerboseForegroundColor = [ConsoleColor]::DarkGreen.ToString();
        $Host.PrivateData.ErrorForegroundColor = [ConsoleColor]::Green.ToString();
        Write-Verbose "Destination Listening: $($dest.server):$($shared.TCPPort) (security label=$($shared.security))";
    }
    catch
    {
        WriteErrorEx $_;
    }
    return $jobInfo;
}

#Starts a backup/restore job from a [SyncJob] object that was previously returned from calling SetupJob( )
function StartJob
{
    param
    (
        [Parameter(Mandatory=$true)][SyncJob]$jobInfo
    )
    try
    {
        $jobInfo.started = $true;

        <##########################
         DESTINATION SERVER SCRIPT
        ###########################>
        $jobInfo.jobDest = Invoke-Command $jobInfo.remoteDest -ScriptBlock {
            Set-StrictMode -Version 1.0;

            #make sure destination folder exists
            for($i = 0; $i -lt $dest.paths.Length; $i++)
            {
                if($dest.paths[$i] -eq $null) { continue; }

                [DatabasePath]$p = $dest.paths[$i];
                switch($p.pathType)
                {
                    1 #File
                    {
                        if(![IO.Directory]::Exists($p.path))
                        {
                            $null = [IO.Directory]::CreateDirectory($p.path);
                        }
                        break;
                    }
                }
            }

            #wait for TCP connections to process. One command per connection. When the remote side is finished, it will send a listener terminate request
            ProcessTCPConnections -ScriptBlock {
                Set-StrictMode -Version 1.0;

                [Net.Sockets.TcpClient]$tcp = $args[0];
                [Net.Sockets.NetworkStream]$networkStream = $args[1];
                [MetaData]$meta = $null;

                try
                {
                    #let far side know we are ready to start handshake
                    $b = [byte[]]::new(1);
                    if(CheckForStopProcessing)
                    {
                        $b[0] = 66;
                    }
                    else
                    {
                        $b[0] = 65;
                    }
                    $networkStream.Write($b, 0, $b.Length);

                    $b = [byte[]]::new(16);
                    if($networkStream.Read($b, 0, $b.Length) -ne $b.Length)
                    {
                        Write-Error "Wrong number of bytes available when pulling security label";
                        return $false;
                    }
                    $g = [guid]::new($b);
                    if($g -ne $shared.security)
                    {
                        Write-Error "Security label mismatch=$g";
                        return $false;
                    }

                    #retrieve command
                    $b = [byte[]]::new(4);
                    $read = $networkStream.Read($b, 0, $b.Length);
                    if($read -ne $b.Length)
                    {
                        throw "Wrong Number of Bytes Available When Pulling Int32 Size";
                    }
                    $i = [BitConverter]::ToInt32($b, 0);
                    if($i -gt 8192) { throw "Command Too Long=$i"; }
                    $b = [byte[]]::new($i);
                    $read = 0;
                    while($read -lt $i)
                    {
                        $newRead = $networkStream.Read($b, $read, $b.Length - $read);
                        if($newRead -le 0) { throw "Cannot Pull Command"; }
                        $read += $newRead;
                    }
                    $command = [Text.Encoding]::UTF8.GetString($b);

                    #obtain subCommand, if command extends beyond just one character
                    [string]$subCommand = $null;
                    if($command.Length -gt 1)
                    {
                        $subCommand = $command.Substring(1);
                        $command = $command.Substring(0, 1);
                    }

                    #old command was only 1 byte
                    #if($networkStream.Read($b, 0, 1) -ne 1) { Write-Error "Command wasn't received"; }
                    #$command = [char]$b[0];

                    Write-Verbose "$($FromServer):$($FromPath): Received Command $($command):$($subCommand)";
                    switch($command)
                    {
                        'X' #exit TCP server
                        {
                            Write-Verbose "$($FromServer):$($FromPath): Closing TCP Listener";
                            return $true; #close listener
                            break;
                        }

                        { $_ -in 'S','T','U' } #process SQL stream
                        {
                            $stopAt = [datetime]::MaxValue;
                            if($subCommand.Length -gt 1) { $stopAt = ([datetime]$subCommand).ToLocalTime(); }
                            $disableRestartOption = if($_ -eq 'T') { $true } else { $false };
                            $enableRestartAfterErrorOption = if($_ -eq 'U') { $true } else { $false };
                            try
                            {
                                $meta = GetMetaData $networkStream;
                                if($meta -eq $null)
                                {
                                    throw "Missing Meta Database";
                                }
                                $null = ReceiveSQLStream $meta $tcp $networkStream $stopAt $disableRestartOption $enableRestartAfterErrorOption;
                            }
                            catch
                            {
                                WriteErrorEx $_ -addToNotification "Database Restore Error: $($FromServer):$($FromPath) -> Sending Abort.";
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "121`r`n"); } catch { }
                                $null = SendOOBCommand $tcp 'A'; #bort
                            }
                            break;
                        }

                        'F' #file copy stream (similar to a SQL stream, except this only copies the file to wherever the metadata tells us to, instead of using the -ToPath argument)
                        {
                            try
                            {
                                $meta = GetMetaData $networkStream;
                                if($meta -eq $null)
                                {
                                    throw "Missing Meta Database";
                                }
                                $null = ReceiveFile $meta $tcp $networkStream;
                            }
                            catch
                            {
                                WriteErrorEx $_;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "122`r`n"); } catch { }
                                $null = SendOOBCommand $tcp 'A'; #bort
                            }
                            break;
                        }

                        'E' #DBExecute, subCommand will contain the SQL Execute command
                        {
                            [System.Data.SqlClient.SqlConnection]$DB = $null;
                            [System.Data.SqlClient.SqlCommand]$CMD = $null;
                            try
                            {
                                #$meta = GetMetaData $networkStream;
                                #if($meta -eq $null) { throw "Missing Meta Database"; }
                                if($subCommand.Length -eq 0) { throw "Missing DBExecute command"; }

                                $DB = [System.Data.SqlClient.SqlConnection]::new("Server=$($dest.paths[0].path);Database=master;Trusted_Connection=true");
                                $DB.Open();

                                Write-Verbose "$($dest.paths[0].path): Running -> $($subCommand)";
                                $CMD = [System.Data.SqlClient.SqlCommand]::new($subCommand, $DB);
                                $executeReturn = $CMD.ExecuteScalar();
                                $CMD.Dispose();
                                if($executeReturn -eq $null) { $executeReturn = ""; }

                                $bExecuteReturn = [Text.Encoding]::UTF8.GetBytes($executeReturn);
                                $b = [BitConverter]::GetBytes($bExecuteReturn.Length);
                                $networkStream.Write($b, 0, $b.Length);
                                $networkStream.Write($bExecuteReturn, 0, $bExecuteReturn.Length);

                                $null = SendOOBCommand $tcp 'D'; #one
                            }
                            catch
                            {
                                WriteErrorEx $_;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "123`r`n"); } catch { }
                                $null = SendOOBCommand $tcp 'A'; #bort
                            }
                            finally
                            {
                                if($DB -ne $null)
                                {
                                    $DB.Dispose();
                                    $DB = $null;
                                }
                            }
                            break;
                        }

                        'B' #binary search/replace in file (i.e. used to change UND path in LDF file before bringing back online)
                        {
                            try
                            {
                                $meta = GetMetaData $networkStream;
                                if($meta -eq $null) { throw "Missing Meta Database"; }
                                if($meta.files.Length -ne 3) { throw "Need three arguments to search/replace file"; }

                                [string]$filePath = $meta.files[0].physicalPath;
                                [string]$search = $meta.files[1].physicalPath;
                                [string]$strReplace = $meta.files[2].physicalPath;

                                RegExReplace $filePath $search ($strReplace + "`0") $false;

                                $null = SendOOBCommand $tcp 'D'; #one;
                            }
                            catch
                            {
                                WriteErrorEx $_;
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "124`r`n"); } catch { }
                                $null = SendOOBCommand $tcp 'A'; #bort
                            }
                            break;
                        }

                        'R' #BEGIN auto restore to date time
                        {
                            $shared.recoveryType = "NORECOVERY"; #force the recoveryType when we run an autorestore, in case we have to restore multiple logfiles, before we recover it back online
                            [System.Data.SqlClient.SqlConnection]$DB = $null;
                            [System.Data.SqlClient.SqlCommand]$CMD = $null;
                            [System.Data.DataTable]$DT = $null;
                            [System.Data.SqlClient.SqlDataAdapter]$DA = $null;
                            try
                            {
                                $meta = GetMetaData $networkStream;
                                if($meta -eq $null) { throw "Missing Meta Database"; }

                                $instance = ($dest.paths[0].path -split "\\")[1];
                                $dataPath,$logPath = GetDefaultSQLPaths $instance;

                                $DB = [System.Data.SqlClient.SqlConnection]::new("Server=$($dest.paths[0].path);Database=master;Trusted_Connection=true");
                                $DB.Open();

                                if($shared.replace)
                                {
                                    #if the -Replace switch is present, make sure database/files do not exist so we won't get an error when we run CREATE DATABASE
                                    try
                                    {
                                        $CMD = [System.Data.SqlClient.SqlCommand]::new("ALTER DATABASE [$($meta.name)] SET SINGLE_USER WITH ROLLBACK IMMEDIATE", $DB);
                                        $null = $CMD.ExecuteNonQuery();
                                        $CMD.Dispose();
                                    }
                                    catch
                                    {
                                        if($DB.State -eq [System.Data.ConnectionState]::Open) { $DB.Close(); }
                                        $DB.Open();
                                    }
                                    try
                                    {
                                        $CMD = [System.Data.SqlClient.SqlCommand]::new("DROP DATABASE [$($meta.name)]", $DB);
                                        $null = $CMD.ExecuteNonQuery();
                                        $CMD.Dispose();
                                    }
                                    catch
                                    {
                                        if($DB.State -eq [System.Data.ConnectionState]::Open) { $DB.Close(); }
                                        $DB.Open();
                                    }
                                    try { $null = Remove-Item "$($dataPath)$($meta.name).mdf" 2> $null; } catch { }
                                    try { $null = Remove-Item "$($logPath)$($meta.name).ldf" 2> $null; } catch { }
                                    try { $null = Remove-Item "$($logPath)$($meta.name)_log.ldf" 2> $null; } catch { }
                                }

                                Write-Verbose "$($dest.server)\$($meta.name): Creating new database";
                                $CMD = [System.Data.SqlClient.SqlCommand]::new("CREATE DATABASE [$($meta.name)]", $DB);
                                $null = $CMD.ExecuteNonQuery();
                                $CMD.Dispose();

                                #retrieve new database's logical file info
                                Write-Verbose "$($dest.server)\$($meta.name): Retrieving logical filenames";
                                $SQLCmd = "SELECT [type],[name],[physical_name],[size],ISNULL(FILEPROPERTY([name],'SpaceUsed'),[size]) AS [used] FROM sys.master_files WHERE database_id=DB_ID('$($meta.name)')";
                                $DT = [System.Data.DataTable]::new();
                                $DA = [System.Data.SqlClient.SqlDataAdapter]::new($SQLCmd, $DB);
                                [void]$DA.Fill($DT);
                                $DA.Dispose();
                                if($DT.Rows.Count -gt 0)
                                {
                                    [string]$undPath = $null;
                                    $meta.files = [MetaFile[]]::new($DT.Rows.Count + 1); #+1 for pseudo UNDO file meta entry
                                    for($i = 0; $i -lt $DT.Rows.Count; $i++)
                                    {
                                        $meta.files[$i] = [MetaFile]::new();
                                        $meta.files[$i].type = $DT.Rows[$i]["type"];
                                        $meta.files[$i].logicalName = $DT.Rows[$i]["name"];
                                        $meta.files[$i].physicalPath = $DT.Rows[$i]["physical_name"];
                                        $meta.files[$i].size = $DT.Rows[$i]["size"] * 8192; #each page is 8k
                                        $meta.files[$i].used = $DT.Rows[$i]["used"] * 8192; #each page is 8k

                                        if($meta.files[$i].type -eq 0)
                                        {
                                            #use same path for UNDO file as the MDF file
                                            $undPath = [IO.Path]::GetDirectoryName($meta.files[$i].physicalPath);
                                            if (-not $undPath.EndsWith("\")) { $undPath += "\"; }
                                        }
                                    }
                                    $DT.Dispose();

                                    #create pseudo meta entry for UNDO file
                                    $i = $meta.files.Length - 1;
                                    $meta.files[$i] = [MetaFile]::new();
                                    $meta.files[$i].type = 9; #made up number for UNDO file
                                    $meta.files[$i].physicalPath = ($undPath + $meta.name + ".UND");

                                    Write-Verbose "$($dest.server)\$($meta.name): Putting database into STANDBY and taking OFFLINE";
                                    $CMD = [System.Data.SqlClient.SqlCommand]::new("ALTER DATABASE [$($meta.name)] SET RECOVERY $($shared.setIncrementalLoggingType);" +
                                                        "BACKUP DATABASE [$($meta.name)] TO DISK='NUL' WITH NO_COMPRESSION,NO_CHECKSUM;" +
                                                        "BACKUP LOG [$($meta.name)] TO DISK='NUL' WITH NO_COMPRESSION,NO_CHECKSUM,NORECOVERY,CONTINUE_AFTER_ERROR;" +
                                                        "RESTORE DATABASE [$($meta.name)] WITH STANDBY='$($undPath)$($meta.name).UND',CONTINUE_AFTER_ERROR;" +
                                                        "ALTER DATABASE [$($meta.name)] SET OFFLINE;", $DB);
                                    $null = $CMD.ExecuteNonQuery();
                                    $CMD.Dispose();

                                    $null = SendOOBCommand $tcp 'D'; #one
                                }
                                else
                                {
                                    $DT.Dispose();
                                    throw "Cannot find new database after creating it: $($meta.name)";
                                }

                                PutMetaData $meta $networkStream; #send meta data with new database logical file info
                            }
                            catch
                            {
                                Write-Error "Database Restore Error: $($dest.server)\$($meta.name) -> $($_.Exception.Message)";
try { [System.IO.File]::AppendAllText("\Powerbak\error.log", "125`r`n"); } catch { }
                                $null = SendOOBCommand $tcp 'A'; #bort
                            }
                            finally
                            {
                                if($DB -ne $null)
                                {
                                    $DB.Dispose();
                                    $DB = $null;
                                }
                            }
                            break;
                        }

                        default
                        {
                            Write-Error "Unknown command: $command";
                            break;
                        }
                    }
                }
                catch
                {
                    if($_ -eq $null) { Write-Verbose "*** NULL EXCEPTION!? ***"; }
                    WriteErrorEx $_;
                    #throw;
                }
                return $false; #don't close listener
            };
        } -AsJob;


        <##########################
            SOURCE SERVER SCRIPT
        ###########################>
        $jobInfo.jobSrc = Invoke-Command $jobInfo.remoteSrc -ScriptBlock {
            Set-StrictMode -Version 1.0;

            try
            {
                [DatabasePath]$p = $source.paths[0];
                [string[]]$nameList = $null;
                switch($p.pathType)
                {
                    0
                    {
                        $nameList = ExpandDatabases $p.path $p.namesWildcard $shared.sinceHours $shared.randomizeOrder;
                        Write-Information "Found $($nameList.Length) Database(s) for $($p.path)";
                        if($nameList.Length -gt 1)
                        {
                            #if we have more than one source database found, make sure the destination is not pointing to a single database, as that could be dangerous
                            #someone might believe they are backing up all their databases, but they are overwriting the same destination over and over
                            #but at the same time, we want to allow someone to be able to copy a single database, and rename it on the destination
                            if(($dest.paths[0].pathType -eq 0 -and ![string]::IsNullOrEmpty($dest.paths[0].namesWildcard)) -or ($dest.paths.length -gt 1 -and $dest.paths[1].pathType -eq 0 -and ![string]::IsNullOrEmpty($dest.paths[1].namesWildcard))) { throw "Destination cannot specify a database name when backing up multiple databases."; }
                        }

                        foreach($name in $nameList)
                        {
                            $continueWithBackups = $true;
                            BackupDatabase $p.path $name ([ref]$continueWithBackups);
                            if(!$continueWithBackups)
                            {
                                #if BackupDatabase tells us to exit, then we exit
                                break;
                            }
                        }
                        break;
                    }

                    1
                    {
                        if($shared.autoRestoreToDateTime -gt [DateTime]::MinValue)
                        {
                            if($dest.paths[0].pathType -ne 0 -or ($dest.paths.length -gt 1 -and ![string]::IsNullOrEmpty($dest.paths[1].path))) { throw "Destination must have exactly one MSSQL: path."; }
                            [DatabasePath]$dp = $dest.paths[0];
                            [string]$destDBName = $dp.namesWildcard;
                            #if([string]::IsNullOrEmpty($destDBName)) { $destDBName = $p.namesWildcard; }
                            Write-Verbose "RESTORE FROM: $($p.path) name=$($p.namesWildcard)";
                            AutoRestoreDatabase $p.path $p.namesWildcard $shared.autoRestoreToDateTime $destDBName;
                        }
                        else
                        {
                            $nameList = ExpandFilenames ($p.path+$p.namesWildcard) $shared.randomizeOrder;
                            foreach($name in $nameList)
                            {
                                SendFile $name;
                            }
                        }
                        break;
                    }
                }
            }
            catch
            {
                WriteErrorEx $_ -addToNotification "$($source.paths[0].path): Error in Source Loop - ";
                #throw;
            }
            finally
            {
                Write-Verbose "$($source.paths[0].path): Signaling destination server to exit...";
                try
                {
                    [Net.Sockets.TcpClient]$tcp = $null;
                    [Net.Sockets.NetworkStream]$networkStream = $null;
                    $tcp,$networkStream = ConnectToDestinationServer 'X' $null;
                    if($tcp -ne $null)
                    {
                        CloseObjects $tcp $networkStream;
                        $tcp = $null;
                        $networkStream = $null;
                    }
                }
                catch
                {
                    WriteErrorEx $_;
                }
            }
        } -AsJob;
    }
    catch
    {
        WriteErrorEx $_ -addToNotification "Database Backup Error: Cannot start $($source.paths[0].path)";
    }
}

function StopAllJobs
{
    param
    (
        [Parameter(Mandatory=$true)][Collections.Generic.List[SyncJob]]$jobList,
        [Parameter(Mandatory=$true)][bool]$cleanup
    )

    try
    {
        foreach($jobInfo in $jobList)
        {
            if($jobInfo.jobSrc -ne $null)
            {
                if($jobInfo.jobSrc.JobStateInfo.State -eq [Management.Automation.JobState]::Running)
                {
                    $Host.PrivateData.VerboseForegroundColor = [ConsoleColor]::Yellow.ToString();
                    Write-Verbose "Stopping Src Job";
                    #$jobInfo.jobSrc.StopJob();
                    Stop-Job $jobInfo.jobSrc;

                    #if($jobInfo.jobSrc.JobStateInfo.State -ne [Management.Automation.JobState]::Running)
                    #{
                    #    Write-Verbose "Src Job Stopped";
                    #}
                    #else
                    #{
                    #    Write-Verbose "Src Job NOT Stopped";
                    #}
                }

                #$Host.PrivateData.VerboseForegroundColor = [ConsoleColor]::DarkRed.ToString();
                #try { Receive-Job $jobInfo.jobSrc; } catch { }

                #if($cleanup)
                #{
                #    Remove-Job $jobInfo.jobSrc;
                #}
            }

            if($jobInfo.jobDest -ne $null)
            {
                if($jobInfo.jobDest.JobStateInfo.State -eq [Management.Automation.JobState]::Running)
                {
                    $Host.PrivateData.VerboseForegroundColor = [ConsoleColor]::Yellow.ToString();
                    Write-Verbose "Stopping Dest Job";
                    #$jobInfo.jobDest.StopJob();
                    Stop-Job $jobInfo.jobDest;

                    #if($jobInfo.jobDest.JobStateInfo.State -ne [Management.Automation.JobState]::Running)
                    #{
                    #    Write-Verbose "Dest Job Stopped";
                    #}
                    #else
                    #{
                    #    Write-Verbose "Dest Job NOT Stopped";
                    #}
                }

                #$Host.PrivateData.VerboseForegroundColor = [ConsoleColor]::DarkGreen.ToString();
                #try { Receive-Job $jobInfo.jobDest; } catch { }

                #if($cleanup)
                #{
                #    Remove-Job $jobInfo.jobDest;
                #}
            }
        }
    }
    catch
    {
        #Write-Verbose $_;
    }
    finally
    {
        if($cleanup)
        {
            foreach($jobInfo in $jobList)
            {
                if($jobInfo.jobSrc -ne $null) { Remove-Job $jobInfo.jobSrc; }
                if($jobInfo.jobDest -ne $null) { Remove-Job $jobInfo.jobDest; }
                if($jobInfo.remoteSrc -ne $null) { Remove-PSSession $jobInfo.remoteSrc; }
                if($jobInfo.remoteDest -ne $null) { Remove-PSSession $jobInfo.remoteDest; }
            }
        }
    }
}


<##########################
     MAIN ENTRY POINT
###########################>
#turn on information stream, we use this instead of Write-Information, so it does not interfere with function return values
$InformationPreference = [System.Management.Automation.ActionPreference]::Continue;

#Check to see if we are registering a scheduled task, because we don't want to run anything, just set it up
if($ScheduledTask -ne $null)
{
    $null = New-ScheduledTaskSettingsSet; #this will generate dynamic types in [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask]
    RegisterScheduledTask @ScheduledTask -Parameters $PSCmdlet.MyInvocation.BoundParameters;
    return;
}

$jobList = [Collections.Generic.List[SyncJob]]::new();
$watchdogList = [Collections.Generic.List[SyncJob]]::new();
[Threading.Mutex]$gracefulStopMutex = $null;
try
{
    $allMetrics = [Metrics]::new();
    $allMetrics.Start = $StartTime;

    <##########################
        SETUP AND START JOBS
    ###########################>
    switch($PSCmdlet.ParameterSetName)
    {
        "FileConfig" #one or more jobs in a config file
        {
            #read job definition, then manually remove /* comments */ before passing it to ConvertFrom-Json
            $jobDefinitions = ([System.Text.RegularExpressions.Regex]::Replace((Get-Content $BackupJobs), "/\*.*?\*/", "")) | ConvertFrom-Json;
            for($j=0; $j -lt $jobDefinitions.Jobs.Length; $j++)
            {
                $jobDef = PSCustomObjectToHashTable $jobDefinitions.Jobs[$j]; #you can't splat a CustomObject, only a HashTable

                #check for a special parameter NoOverrides in a job entry before adding/overriding parameters set from the command line
                if($jobDef["NoOverrides"] -ne "true")
                {
                    #add/override arguments set from the command line
                    $MyInvocation.BoundParameters.Keys.ForEach({
                        if($_ -ne "BackupJobs" -and $_ -ne "ParallelJobs" -and $_ -ne "EmailNotification" -and (Test-Path "variable:$($_)")) #sip the BackupJobs parameter, 
                        {
                            $null = Invoke-Expression "`$jobDef['$($_)'] = `$$($_)";
                        }
                    });
                }
                $jobDef.Remove("NoOverrides");
                if($jobDef.ContainsKey("EmailNotification")) { $jobDef.Remove("EmailNotification"); }

                #Write-Information @jobDef;

                #setup the job (use splatting) and add it to the job list
                [SyncJob]$newJob = SetupJob @jobDef;
                $jobList.Add($newJob); #setup the job (use splatting) and add it to the job list

                $p = $newJob.source.paths[0].path;
                if(![string]::IsNullOrEmpty($p)) { Write-Information "Setup Complete For $($p)"; }
            }
            break;
        }

        "VSSWatchdog"
        {
            VSSWatchdog $VSSWatchdog;
            return;
        }

        default #single job from command line parameters
        {
            $jobList.Add((SetupJob @PSBoundParameters));
            break;
        }
    }

    [bool]$atLeastOneJob = $false;
    for($j=0; $j -lt $jobList.Count; $j++)
    {
        $jobInfo = $jobList[$j];
        if($jobInfo -eq $null) { continue; }
        if(![string]::IsNullOrEmpty($jobInfo.source.server) -and ![string]::IsNullOrEmpty($jobInfo.dest.server))
        {
            Write-Information "Processing $($jobInfo.source.server) -> $($jobInfo.dest.server) for path: $($jobInfo.source.paths[0].path)\$($jobInfo.source.paths[0].namesWildcard)";
            #Write-Information "Starting synchronization...";
            $atLeastOneJob = $true;
        }
    }


    <##########################
           VSS WATCHDOG
    ###########################>
    $targetServers = [Collections.Generic.Dictionary[string, Management.Automation.Runspaces.PSSession]]::new();
    $currentJobCount = $jobList.Count;
    for($j=0; $j -lt $jobList.Count; $j++)
    {
        $jobInfo = $jobList[$j];
        if($jobInfo.shared.VSSWatchdog -gt 0L)
        {
            #gather list of unique target server paths
            if(![string]::IsNullOrEmpty($jobInfo.dest.server) -and $jobInfo.remoteDest -ne $null)
            {
                if(!$targetServers.ContainsKey($jobInfo.dest.server))
                {
                    $targetServers[$jobInfo.dest.server] = $null;
                    $holdVerbose = $VerbosePreference;
                    #TODO: temporarily force verbose logging of VSSwatchdog
                    $VerbosePreference = [System.Management.Automation.ActionPreference]::Continue;
                    $watchdogInfo = SetupJob -ToServer $jobInfo.dest.server -ToPath "..." -VSSWatchDog $jobInfo.shared.VSSWatchdog -RunVSSWatchdogSession;
                    #TODO: put verbose mode back to what it was
                    $VerbosePreference = $holdVerbose;
                    $watchdogList.Add($watchdogInfo);
                    $watchdogInfo.jobDest = Invoke-Command $watchdogInfo.remoteDest {
                        Set-StrictMode -Version 1.0;

                        VSSWatchdog $shared.VSSWatchdog;
                    } -AsJob;
                }
            }
        }
    }
    $targetServers.Clear();

    #if paralleljobs is a 0, run all jobs simultaneously
    if($ParallelJobs -eq 0)
    {
        $ParallelJobs = $jobList.Count;
    }


    <##########################
            WAIT LOOP
    ###########################>
    #loop and display output from each job until they are all completed
    [console]::TreatControlCAsInput = $true;
    $completed = 0;
    $running = 0;
    $notRunning = 0;
    $gracefulStop = $false;
    while($completed -lt $jobList.Count)
    {
        if($Host.UI.RawUI.KeyAvailable -and ([int]$Host.UI.RawUI.ReadKey("AllowCtrlC,IncludeKeyUp,IncludeKeyDown,NoEcho").Character) -eq 3)
        {
            if(!$gracefulStop)
            {
                $gracefulStop = $true;

                $wasCreated = $false;
                $gracefulStopMutex = [Threading.Mutex]::new($true, $script:gracefulStopMutexName, [ref]$wasCreated);

                Write-Information "Shutting down gracefully... Please allow current backups to finish to avoid corruption.";
                Start-Sleep -Milliseconds 1000;
                $Host.UI.RawUI.FlushInputBuffer();
            }
            else
            {
                Write-Information "Forcing shutdown... Good luck...";
                Start-Sleep -Milliseconds 1000;
                $Host.UI.RawUI.FlushInputBuffer();
                break;
            }
        }

        if($gracefulStop -and $running -eq 0)
        {
            #if a graceful stop was requested, and the running count hits 0, then we are done
            break;
        }

        Start-Sleep -Milliseconds 100;

        $completed = 0;
        $running = 0;
        $notStarted = 0;
        #display progress for backup/restore jobs
        for($j=0; $j -lt $jobList.Count; $j++)
        {
            $jobInfo = $jobList[$j];

            if($jobInfo.shared.maintenanceOnly)
            {
                #count maintenace jobs as completed so we don't get stuck in this loop, as they do not run here, they are information for the last process below
                $completed++;
                continue;
            }

            if(!$jobInfo.started)
            {
                #this job hasn't been started yet, continue looping
                $notStarted++;
                continue;
            }

            if($jobInfo.jobSrc.JobStateInfo.State -ne [Management.Automation.JobState]::Running -and $jobInfo.jobDest.JobStateInfo.State -ne [Management.Automation.JobState]::Running)
            {
                #keep counter of completed jobs
                $completed++;
            }
            else
            {
                #keep counter of running jobs
                $running++;
            }

            #if($jobInfo.jobSrc.JobStateInfo.State -eq [Management.Automation.JobState]::Failed)
            #{
            #    Write-Information "Job Failed at Src!!";
            #}

            #if($jobInfo.jobDest.JobStateInfo.State -eq -[Management.Automation.JobState]::Failed)
            #{
            #    Write-Information "Job Failed at Dest!!";
            #}

            if($jobInfo.jobSrc.HasMoreData)
            {
                $Host.PrivateData.VerboseForegroundColor = [ConsoleColor]::DarkRed.ToString();
                $Host.PrivateData.ErrorForegroundColor = [ConsoleColor]::Red.ToString();
                Receive-Job $jobInfo.jobSrc;
                Start-Sleep -Milliseconds 10;
            }

            if($jobInfo.jobDest.HasMoreData)
            {
                $Host.PrivateData.VerboseForegroundColor = [ConsoleColor]::DarkGreen.ToString();
                $Host.PrivateData.ErrorForegroundColor = [ConsoleColor]::Green.ToString();
                Receive-Job $jobInfo.jobDest;
                Start-Sleep -Milliseconds 10;
            }
        }

        #start jobs if we are below the parallel threshold, as previous jobs finish
        if(!$gracefulStop -and $running -lt $ParallelJobs -and $notStarted -gt 0)
        {
            for($r=$running; $r -lt $ParallelJobs; $r++)
            {
                for($j=0; $j -lt $jobList.Count; $j++)
                {
                    $jobInfo = $jobList[$j];
                    if($jobInfo.shared.maintenanceOnly) { continue; }

                    if(!$jobInfo.started)
                    {
                        StartJob $jobInfo;
                        break;
                    }
                }
            }
        }

        #display progress for VSSWatchdog
        for($j=0; $j -lt $watchdogList.Count; $j++)
        {
            $jobInfo = $watchdogList[$j];

            if($jobInfo.jobDest.HasMoreData)
            {
                $Host.PrivateData.VerboseForegroundColor = [ConsoleColor]::White.ToString();
                $Host.PrivateData.ErrorForegroundColor = [ConsoleColor]::White.ToString();
                Receive-Job $jobInfo.jobDest;
                Start-Sleep -Milliseconds 10;
            }
        }
    }
    #$j.EndInvoke($invoke);
    [console]::TreatControlCAsInput = $false;


    <##########################
    FORCE STOP ANY LINGERING JOBS
    ###########################>
    if($watchdogList -ne $null) { StopAllJobs $watchdogList $false; }
    StopAllJobs $jobList $false;
    Write-Information "All Jobs Are Stopped.";


    <##########################
     COLLECT JOB METRICS/ERRORS
    ###########################>
    $reportSpacer = "-------------------------------------------------";
    [string]$jobResults = $null;
    if($atLeastOneJob)
    {
        #collect dest metrics and database errors for email report
        for($j=0; $j -lt $jobList.Count; $j++)
        {
            $jobInfo = $jobList[$j];

            if($jobInfo.shared.maintenanceOnly) { continue; }

            $errorList = [Collections.Generic.List[string]]::new();

            if($jobInfo.jobDest -ne $null)
            {
                #add remote metrics from destination into allMetrics
                try
                {
                    $remoteMetrics = Invoke-Command $jobInfo.remoteDest {
                        return $shared.metrics;
                    };
                    $allMetrics | Get-Member -MemberType Properties | ForEach-Object { if($_.Definition -notmatch "^datetime") { $allMetrics."$($_.Name)" += $remoteMetrics."$($_.Name)"; }}

                    #pull errors from destination that begin with "Database Restore Error:"
                    foreach($e In $jobInfo.jobDest.ChildJobs)
                    {
                        foreach($m in $e.Error)
                        {
                            if($m.Exception.Message.Length -ge 23 -and $m.Exception.Message -match "^Database (?:Backup|Restore) Error:")
                            {
                                $errorList.Add($m.Exception.Message);
                            }
                        }
                    }
                }
                catch {} #ignore errors here
            }

            #collect src metrics and database errors for email report
            if($jobInfo.jobSrc -ne $null)
            {
                try
                {
                    #add remote metrics from source into allMetrics
                    $remoteMetrics = Invoke-Command $jobInfo.remoteSrc {
                        return $shared.metrics;
                    };
                    $allMetrics | Get-Member -MemberType Properties | ForEach-Object { if($_.Definition -notmatch "^datetime") { $allMetrics."$($_.Name)" += $remoteMetrics."$($_.Name)"; }}

                    #pull errors from source that begin with "Database Backup Error:"
                    foreach($e In $jobInfo.jobSrc.ChildJobs)
                    {
                        foreach($m in $e.Error)
                        {
                            if($m.Exception.Message.Length -ge 23 -and $m.Exception.Message -match "^Database (?:Backup|Restore) Error:")
                            {
                                $errorList.Add($m.Exception.Message);
                            }
                        }
                    }
                }
                catch {} #ignore errors here
            }

            if($errorList.Count -gt 0)
            {
                $jobResults += "$($jobList[$j].source.paths[0].path) -> $($jobList[$j].dest.server):`r`n$($reportSpacer)`r`n$([string]::Join("`r`n", $errorList))`r`n`r`n`r`n$($reportSpacer)`r`n";
            }
        }
        if([string]::IsNullOrEmpty($jobResults)) { $jobResults = "No failures!"; }
    }


    <##########################
        MAINTENACE TASKS
    ###########################>
    #purge backup history after job
    $targetServers.Clear();
    for($j=0; $j -lt $jobList.Count; $j++)
    {
        $jobInfo = $jobList[$j];
        if($jobInfo.shared.purgeBackupHistoryDays -gt 0)
        {
            #run purge on source server's SQL instances
            [DatabasePath[]]$paths = $null;
            [Management.Automation.Runspaces.PSSession]$session = $null;
            if(![string]::IsNullOrEmpty($jobInfo.source.server) -and $jobInfo.remoteSrc -ne $null)
            {
                $paths = $jobInfo.source.paths;
                for($i = 0; $i -lt $paths.Length; $i++)
                {
                    [DatabasePath]$p = $paths[$i];
                    if($p -ne $null -and $p.pathType -eq 0) #0=database
                    {
                        if($targetServers.ContainsKey($p.path) -and $i -gt 0)
                        {
                            $targetServers[$p.path] = $null;
                        }
                        else
                        {
                            $targetServers[$p.path] = $jobInfo.remoteSrc;
                        }
                    }
                }
            }
            #run purge on dest server's SQL instances
            if(![string]::IsNullOrEmpty($jobInfo.dest.server) -and $jobInfo.remoteDest -ne $null)
            {
                $paths = $jobInfo.dest.paths;
                for($i = 0; $i -lt $paths.Length; $i++)
                {
                    [DatabasePath]$p = $paths[$i];
                    if($p -ne $null -and $p.pathType -eq 0) #0=database
                    {
                        if($targetServers.ContainsKey($p.path) -and $i -gt 0)
                        {
                            $targetServers[$p.path] = $null;
                        }
                        else
                        {
                            $targetServers[$p.path] = $jobInfo.remoteDest;
                        }
                    }
                }
            }
        }
    }
    if($targetServers.Count -gt 0)
    {
        Write-Information "`r`n$($reportSpacer)`r`nPURGE BACKUP HISTORY:`r`n$($reportSpacer)";
        Invoke-Command ([Management.Automation.Runspaces.PSSession[]]($targetServers.Values | ? { $_; })) {
            Set-StrictMode -Version 1.0;

            if($shared.purgeBackupHistoryDays -gt 0)
            {
                [DatabasePath[]]$paths = $null;
                if($shared.isDestination)
                {
                    $paths = $dest.paths;
                }
                else
                {
                    $paths = $source.paths;
                }
                for($i = 0; $i -lt $paths.Length; $i++)
                {
                    [DatabasePath]$p = $paths[$i];
                    if($p -ne $null -and $p.pathType -eq 0) #0=database
                    {
                        Write-Information "$($p.path): Purging backup history older than $($shared.purgeBackupHistoryDays) days...";
                        PurgeBackupHistory $p.path $shared.purgeBackupHistoryDays;
                    }
                }
            }
        };

    }

    #purge unused databases on target servers (never on source)
    $targetServers.Clear();
    for($j=0; $j -lt $jobList.Count; $j++)
    {
        $jobInfo = $jobList[$j];
        if($jobInfo.shared.deleteAfterNoBackupRestoreDays -gt 0)
        {
            #run purge on dest server's SQL instances
            if(![string]::IsNullOrEmpty($jobInfo.dest.server) -and $jobInfo.remoteDest -ne $null)
            {
                $paths = $jobInfo.dest.paths;
                for($i = 0; $i -lt $paths.Length; $i++)
                {
                    [DatabasePath]$p = $paths[$i];
                    if($p -ne $null -and $p.pathType -eq 0) #0=database
                    {
                        if($targetServers.ContainsKey($p.path) -and $i -gt 0)
                        {
                            $targetServers[$p.path] = $null;
                        }
                        else
                        {
                            $targetServers[$p.path] = $jobInfo.remoteDest;
                        }
                    }
                }
            }
        }

        if($jobInfo.shared.deleteOldFiles -gt 0)
        {
            #run purge on dest server's SQL instances
            if(![string]::IsNullOrEmpty($jobInfo.dest.server) -and $jobInfo.remoteDest -ne $null)
            {
                $paths = $jobInfo.dest.paths;
                for($i = 0; $i -lt $paths.Length; $i++)
                {
                    [DatabasePath]$p = $paths[$i];
                    if($p -ne $null -and $p.pathType -eq 1) #1=filepath
                    {
                        if($targetServers.ContainsKey($p.path) -and $i -gt 0)
                        {
                            $targetServers[$p.path] = $null;
                        }
                        else
                        {
                            $targetServers[$p.path] = $jobInfo.remoteDest;
                        }
                    }
                }
            }
        }
    }
    if($targetServers.Count -gt 0)
    {
        Write-Information "`r`n$($reportSpacer)`r`nPURGE NOACTIVITY DATABASES:`r`n$($reportSpacer)";
        Invoke-Command ([Management.Automation.Runspaces.PSSession[]]($targetServers.Values | ? { $_; })) {
            if($shared.deleteAfterNoBackupRestoreDays -gt 0)
            {
                for($i = 0; $i -lt $dest.paths.Length; $i++)
                {
                    [DatabasePath]$p = $dest.paths[$i];
                    if($p -ne $null -and $p.pathType -eq 0) #0=database
                    {
                        Write-Information "$($p.path): Purging unused databases with no backup/restore activity for $($shared.deleteAfterNoBackupRestoreDays) days...";
                        PurgeOldDatabases $p.path $shared.deleteAfterNoBackupRestoreDays;
                    }
                }
            }

            if($shared.deleteOldFiles -gt 0)
            {
                for($i = 0; $i -lt $dest.paths.Length; $i++)
                {
                    [DatabasePath]$p = $dest.paths[$i];
                    if($p -ne $null -and $p.pathType -eq 1) #1=filepath
                    {
                        Write-Information "$($p.path): Purging old backup files older than $($shared.deleteOldFiles) days...";
                        DeleteOldFiles $p.path $shared.deleteOldFiles;
                    }
                }
            }

        };
    }


    <##########################
        SNAPSHOT MANAGEMENT
    ###########################>
    [string]$snapshotResults = $null;
    for($j=0; $j -lt $jobList.Count; $j++)
    {
        $jobInfo = $jobList[$j];

        $snap = $jobInfo.shared.snapshotManagement;
        if($snap -eq $null -or $jobInfo.remoteDest -eq $null) { continue; }

        $snapshotVolume = $snap["Volume"];
        if(![string]::IsNullOrEmpty($snapshotVolume))
        {
            $snapshotDaysOfMonth = [int[]]$snap["DaysOfMonth"];
            $snapshotIntervalDays = [int]$snap["IntervalDays"];
            $snapshotDeleteAfterDays = [int]$snap["DeleteAfterDays"];
            $snapshotExpose = [string]$snap["Expose"];

            if(![string]::IsNullOrEmpty($snapshotExpose))
            {
                if($snapshotExpose[$snapshotExpose.Length - 1] -ne "\") { $snapshotExpose += "\"; }

                #expand any expression in path
                $posStart = $snapshotExpose.IndexOf("{");
                $posEnd = $snapshotExpose.IndexOf("}");
                if($posStart -gt -1 -and $posEnd -gt -1)
                {
                    $cmd = $snapshotExpose.Substring($posStart + 1, $posEnd - $posStart - 1);
                    $strReplace = Invoke-Expression $cmd;
                    $snapshotExpose = ($snapshotExpose.Substring(0, $posStart) + $strReplace + $snapshotExpose.Substring($posEnd + 1));
                }

                #add automatic date to path if requested
                #Note: use [datetime]::Now, not $StartTime for creating snapshot foldername, because this is done AFTER the backup
                if($jobinfo.shared.appendDateTimeFolderToPath -and ![string]::IsNullOrEmpty($script:autoFolderDateTime)) { $snapshotExpose += ([datetime]::Now.ToString($script:autoFolderDateTime) + "\"); }
            }

            [string]$curSnapshotStatus = $null;
            [bool]$skip = $false;
            if($snapshotIntervalDays -gt 0)
            {
                #see if any existing snapshots exist within the last intervalDays, so we don't create too many
                $existingSnapshots = SnapshotList $snapshotVolume;
                $compareDate = $StartTime.Date.AddDays(-$snapshotIntervalDays);
                [Alphaleonis.Win32.Vss.VssSnapshotProperties]$existingSnapshot = $null;
                foreach($existingSnapshot in $existingSnapshots)
                {
                    if($existingSnapshot.CreationTimestamp -ge $compareDate)
                    {
                        $skip = $true;
                        break;
                    }
                }
            }
            elseif($StartTime.Day -in $snapshotDaysOfMonth)
            {
                #see if any existing snapshots were already created today, so we don't create two
                $existingSnapshots = SnapshotList $snapshotVolume;
                [Alphaleonis.Win32.Vss.VssSnapshotProperties]$existingSnapshot = $null;
                foreach($existingSnapshot in $existingSnapshots)
                {
                    if($existingSnapshot.CreationTimestamp.Date -eq $StartTime.Date)
                    {
                        $skip = $true;
                        break;
                    }
                }
            }
            else
            {
                $skip = $true;
            }

            $curSnapshotStatus = Invoke-Command $jobInfo.remoteDest {
                $skip = $using:skip;
                $snapshotVolume = $using:snapshotVolume;
                $snapshotExpose = $using:snapshotExpose;
                $snapshotDaysOfMonth = $using:snapshotDaysOfMonth;
                $snapshotDeleteAfterDays = $using:snapshotDeleteAfterDays;
                if(![string]::IsNullOrEmpty($snapshotVolume))
                {
                    SetVolumeProtectionLevel $snapshotVolume; #protect snapshot from being deleted
                    if($snapshotDeleteAfterDays -gt 0)
                    {
                        $deletionCount = SnapshotDelete $snapshotVolume $snapshotDeleteAfterDays;
                        Write-Information "Deleted $($deletionCount) old volume snapshot(s)`r`n";
                    }
                    if($skip)
                    {
                        Write-Information "No volume snapshot created for $($snapshotVolume)`r`n";
                    }
                    else
                    {
                        $snap = SnapshotCreate $snapshotVolume;
                        if($snap -ne $null)
                        {
                            Write-Information "Volume snapshot created for $($snapshotVolume)`r`n";
                            if(![string]::IsNullOrEmpty($snapshotExpose))
                            {
                                Write-Information "Volume snapshot exposed at $($snapshotExpose)`r`n";
                                SnapshotExpose $snap $snapshotExpose;
                            }
                        }
                    }
                }
            };

            $snapshotResults += "$($jobInfo.dest.server):`r`n$($curSnapshotStatus)";
        }
    }
    if(![string]::IsNullOrEmpty($snapshotResults))
    {
        $snapshotResults = "$($reportSpacer)`r`nVOLUME SNAPSHOT CHANGES:`r`n$($reportSpacer)`r`n$($snapshotResults)";
        Write-Information "`r`n$($snapshotResults)";
    }


    <##########################
        ALL TASKS COMPLETED
    ###########################>
    $allMetrics.End = [datetime]::Now;
    $jobResults = ("$($reportSpacer)`r`nJOB METRICS:`r`n$($reportSpacer)`r`n$(($allMetrics | Out-String).Trim())`r`n" +
                    "`r`n$($reportSpacer)`r`nJOB RESULTS:`r`n$($reportSpacer)`r`n$($jobResults)");
    Write-Information "`r`n$($jobResults)";


    <##########################
         EMAIL RESULTS
    ###########################>
    if($EmailNotification -and ![string]::IsNullOrEmpty($jobNotificationToEmails))
    {
        $paramList = "$($reportSpacer)`r`nPARAMETERS:`r`n$($reportSpacer)`r`n$($PSBoundParameters | ConvertTo-Json)`r`n";

        if(![string]::IsNullOrEmpty($SMTPUsername))
        {
            [pscredential]$emailCred = $null;
            $emailCred = [pscredential]::new($SMTPUsername, (ConvertTo-SecureString $SMTPPassword -AsPlainText -Force));
            Send-MailMessage -To $jobNotificationToEmails.Split("[,;]") -From $jobNotificationFromEmail -Subject $jobNotificationSubject -Body "$($paramList)$($snapshotResults)$($jobResults)" -SmtpServer $SMTPServer -Port $SMTPPort -UseSsl -Credential $emailCred;
        }
        else
        {
            Send-MailMessage -To $jobNotificationToEmails.Split("[,;]") -From $jobNotificationFromEmail -Subject $jobNotificationSubject -Body "$($paramList)$($snapshotResults)$($jobResults)" -SmtpServer $SMTPServer -Port $SMTPPort;
        }
    }
}
catch
{
    WriteErrorEx $_;
    #throw;
}
finally
{
    <##########################
             CLEANUP
    ###########################>
    [console]::TreatControlCAsInput = $true; #do not allow breaking out of this finally

    $Host.PrivateData.VerboseForegroundColor = [ConsoleColor]::Yellow.ToString();
    Write-Verbose "Cleaning Up Remote Sessions";

    if($gracefulStopMutex -ne $null)
    {
        $gracefulStopMutex.Close();
        $gracefulStopMutex.Dispose();
        $gracefulStopMutex = $null;
    }

    if($watchdogList -ne $null) { StopAllJobs $watchdogList $true; }
    StopAllJobs $jobList $true;

    [console]::TreatControlCAsInput = $false;
}
