cd /istock

echo '--------------------------------------------'
echo message process 
echo 'start at:' `date` 
export PATH=/root/anaconda2/bin:/usr/lib64/qt-3.3/bin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin:/usr/local/bin
export ORACLE_HOME=/opt/instantclient_10_2
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME:/usr/local/lib:/usr/lib

cnt=`ps -ef|grep $0|wc|awk '{print $1}'`
if [ $cnt -gt 4 ]; then
  echo 'process still running!' $cnt $0
  exit
fi

python msg.py

echo 'end at:' `date`
