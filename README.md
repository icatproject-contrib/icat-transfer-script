#ICAT Transfer Script

##About
The aim of this script is to enable users to move specified entities from one ICAT server to another.

##Requirements
Python 2.7 with modules:
- [python-icat](https://icatproject.org/user-documentation/python-icat/)
  - suds
- requests

##Use
###Configuring the script
Before executing the script, the user should fill out the config file (config.ini) with the server login info for the servers you wish to transfer between. For example:
```
; Login data for the ICAT you want to export data from
[export]
url: https://icat1.example.com
auth: simple
username: username1
password: password1

; Login data for the ICAT you want to import to
[import]
url: https://icat2.example.com
auth: simple
username: username2
password: username2
```

###Starting the script
After the user has configured the script, they should start the script either through the command line with the current directory as the script folder, or using the .bat file in the folder. There are multiple arguments available for use with the script, as listed below.

####Positional Arguments

#####query
Defines the ICAT entity to be transferred.

#####duplicate{throw, ignore, check, overwrite}
Defines the action to be taken when a duplicate is found:
- throw: Throws an exception if a duplicate's found.
- ignore: Go to the next row.
- check: Checks that the old database matches the new - throws an exception if it does not.
- overwrite: Replaces old data with the new.

#####maxEntities _(This is only available when operating on ICAT v4.5.1 servers or older, new versions grab limit)_
The number of entities you wish to transfer in one chunk (ICAT servers have limits to how many can be transferred at once, code will throw an exception if over limit).

####Optional arguments

#####-h, --help
Shows the help message.

#####-all
Attempts to transfer all fields (by default values for modId,createId, modDate and createDate will not be transferred). Will throw an exception if user is not specified in the rootUserNames in the icat.properties file.