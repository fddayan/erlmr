ps aux|grep beam|grep -v grep|awk '{print $2}'|xargs kill
