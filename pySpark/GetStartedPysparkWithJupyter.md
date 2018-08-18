## pyspark with jupyter環境架設

Date: 2017/10/30  
Author: 柯則宇

本篇文章的目的是要讓pyspark可以在jupyter上進行互動式coding介面。雖然spark本身是用scala開發，且scala運行速度要比python快，但python對資料處理容易、學習曲線平緩，且有眾多的libraries支援，因此python還是大多數資料科學家的首選。有關python和scala的比較可以看[這篇文章](https://www.dezyre.com/article/scala-vs-python-for-apache-spark/213)。  
* 本篇的OS環境為CentOS-7  
* spark是本機執行模式

未來有機會架在Hadoop上時會再補上。

### 安裝 Oracle JDK

從[Oracle官網](http://www.oracle.com/technetwork/java/javase/downloads/index.html)選擇JDK版本的下載連結，並透過下列cookie設置來下載：

	wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u151-b12/e758a0de34e24606bca991d704f6dcbf/jdk-8u151-linux-x64.rpm"

安裝JDK:

	rpm -ivh jdk-8u151-linux-x64.rpm

設定java環境變數：

	nano /etc/environment
	 JAVA_HOME=/usr/java/jdk1.8.0_151
	source /etc/environment
	echo $JAVA_HOME

### 安裝 spark

從[spark官網](https://spark.apache.org/downloads.html)下載spark。由於spark會和Hadoop溝通，所以如果要讓spark在yarn上執行的話，則需要在下載spark時，按照所依附的Hadoop版本來選擇：

	wget http://ftp.twaren.net/Unix/Web/apache/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz
	tar -xzv -f spark-2.2.0-bin-hadoop2.7.tgz -C /usr/local/

設定spark環境變數：

	nano ~/.bashrc
	 export SPARK_HOME=/usr/local/spark-2.2.0-bin-hadoop2.7
	 export PATH=$PATH:$SPARK_HOME/bin
	source ~/.bashrc

### 透過 anaconda 安裝 python3

可以從[anaconda官網](https://www.anaconda.com/download/)選擇版本並下載anaconda：

	wget https://repo.continuum.io/archive/Anaconda3-5.0.1-Linux-x86_64.sh
	bash Anaconda3-5.0.1-Linux-x86_64.sh

1. 安裝過程將會提示"In order to continue the installation process, please review the license agreement."。鍵入Enter可以查看license。
2. 滾動到license底部輸入`yes`即可繼續安裝。
3. 安裝程序會提示點擊Enter接受默認安裝位置、CTRL-C取消安裝，或者指定備用安裝路徑。若使用默認安裝位置程序將顯示"PREFIX=/home/<user>/anaconda3"，並繼續安裝。可能需要幾分鐘時間才能完成。
4. 安裝程序會提示是否要在當前用戶設定環境變數，輸入`yes`確定。如果輸入`no`的話，則需自己在設定anaconda的環境變數`export PATH="/your/path/anaconda3/bin:$PATH"`。
5. source環境變數。

安裝成功後可執行python3、jupyter等指令。

### 設定防火牆

設定防火牆，讓特定ip可通過：

	firewall-cmd --zone=public --add-rich-rule='rule family="ipv4" source address="192.168.0.0/24" accept' --permanent
	firewall-cmd --reload

### 設定使用 jupyter 開啟 pyspark

我們將直接開啟jupyter notebook，並透過findspark package在代碼中創建一個Spark Session。  
findspark可以在任何IDE中被import，因此這方法並不限定於使用jupyter。  
安裝findspark：

	pip install findspark

啟動jupyter：

	jupyter notebook

接著創建一個python3 notebook，然後運行下列python代碼：

	import findspark
	findspark.init()
	import pyspark
	import random
	ss = pyspark.sql.SparkSession.builder \
		.master('local[1]') \
		.appName("MonteCarloPi") \
		.getOrCreate()
	ss

	num_samples = 100000000
	def inside(p):     
	    x, y = random.random(), random.random()
	    return x*x + y*y < 1
	count = ss.SparkContext.parallelize(range(0, num_samples)).filter(inside).count()
	pi = 4 * count / num_samples
	print(pi)

	ss.stop()

當然，你也可以在創建完Spark Session後，進行你所需要的代碼。

### 設定 jupyter 遠端連線

啟用jupyter時加上`--ip='*'`，可讓localhost以外的機器也能使用jupyter：

	jupyter notebook --ip='*'

但此時的做法是沒有權限上的管理，也就是說，任何人只要知道jupyter notebook的網址，就能進行連線。
因此還需要設置密碼以及SSL。  
編輯jupyter_notebook_config.py設定密碼，預設位置為`~/.jupyter/jupyter_notebook_config.py`。若沒有jupyter_notebook_config.py可先執行下列指令來產生：

	jupyter notebook --generate-config

由於在jupyter_notebook_config.py內所用的密碼並非明碼，而是hash過後的值，因此我們要先轉換密碼。可透過python來轉換：

	ptyhon3
	 >>>from notebook.auth import passwd
	 >>>passwd()
	    Enter password:
		Verify password:
		'sha1:014c4...<your hashed password here>'

執行jupyter_notebook_config.py：

	nano jupyter_notebook_config.py
	 # 在檔案中找到c.NotebookApp.password，拿掉註解並在最後貼上剛剛產生的hash密碼
	 c.NotebookApp.password = u'sha1:014c4...<your hashed password here>'

如此密碼便設定完成。  
接下來設定SSL。首先輸入下列指令產生mykey.key檔以及mycert.pem檔：

	openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout mykey.key -out mycert.pem

在jupyter_notebook_config.py中添加設定：

	nano jupyter_notebook_config.py
	 c.NotebookApp.certfile = u'/absolute/path/to/your/certificate/mycert.pem'
	 c.NotebookApp.keyfile = u'/absolute/path/to/your/certificate/mykey.key'
	 .NotebookApp.ip = '*'
	 c.NotebookApp.password = u'sha1:014c4...<your hashed password here>'
	 c.NotebookApp.open_browser = False
	 # 如果port衝到，也可在這邊修改
	 c.NotebookApp.port = 8888

之後只要輸入`jupyter notebook`即可。此時jupyter不會在本機開啟browser，我們必須在遠端自行輸入網址：

	https://<your.ip>:8888

**注意：**這邊網址中的`https://`不可省略，亦不可以用`http://`代替。  
完成後就會進入輸入密碼的畫面，輸入密碼後就完成jupyter遠端連線了。

### 參考資料

* python for spark or scala for spark的比較  
https://www.dezyre.com/article/scala-vs-python-for-apache-spark/213
* 使用Jupyter Notebook 開啟pyspark  
https://blog.sicara.com/get-started-pyspark-jupyter-guide-tutorial-ae2fe84f594f
* jupyter遠端連線設定(原文)  
http://jupyter-notebook.readthedocs.io/en/4.x/public_server.html
* jupyter遠端連線設定(中文)  
http://pre.tir.tw/008/blog/output/jupyter-notebook-an-quan-xing-she-ding-mi-ma-ssl.html