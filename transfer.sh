# checking for lock file
if [ -e "cronjob/lock" ]
then
echo " lock is there qutting "
exit 0
else
echo "No lock .. going to code"
touch cronjob/lock
fi

. ./classpath


 java -classpath  $CLASSPATH ArchiverAuto 



# setting aArchiveName in config.properties file




rm cronjob/lock

