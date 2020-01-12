##########
#  UNIX  #
##########


scp with folders and files
==========================
scp -r SOURCE_PATH username@SERVER_2:DESTINATION_PATH

scp -C SOURCE_PATH username@SERVER_2:DESTINATION_PATH --> compress and send (use for large files)

Note: first we have to connect to SERVER_1 and run above command in SERVER_1

SFTP:
=====
sftp username@remoteserver_1

After we can use mget or mput based on our requirement.

to fetch file from remoteserver_1.
cd path
mget ./file_path

to put file into remoteserver_1
----------------------------------
mput ./file_path


to check files count recursively in unix
============================================
find dir_name -type f | wc -l


run below command in destination (DESTINATION)
==============================================
hadoop distcp -overwrite SOURCE DESTINATION

to run unix process in Backend:
===============================
nohup ot tmux (to run backend)
	

MAVEN:
=======
to create the jar file for maven project:
Right click on project -->  RUN AS --> Maven Install   (check jar file in workspace\Project\target  folder)


UNIX HINTS
============
to find line numbers for specific word: grep -i -n 'word' filename ==> grep -i -n 'CLM_ACES_RD138092' clm_aces_metadata.json
to print specified lines of text: sed -n '<start_line>,<endline>p' file ==> ex: sed -n '5500,5700p' clm_aces_metadata.json
to know path of file : Locate filename
If your cursor is on the first line (if not, type: gg or 1G ), then you can just use dG . It will delete all lines from the current line to the end of file.


unix functions example:
==========================
functionName "arg1" #It will fail because "functionName" is not declared. we can call function once it is declared.
functionName()
{
	echo $1   #returning value in unix
}	
value=$(functionName "arg1") 
echo value #it will print.

to set indent in unix script:
==============================
unix auto indent:  -- :set autoindent

Excel DATE FUNCTIONS:
=============================
unix epoch to datetime conversion in excel :
Cell/86400+25569 (if epoch time is 10 digits or have only secs) --> ex: A2/86400+25569
Cell/86400000+25569 (if epoch time is 13 digits or have micro secs) --> ex: A2/86400000+25569
