## pysaprk DataFrame

Date: 2017/11/11  
Author: 柯則宇

本篇主要是要熟悉Spark DataFrame，有使用一些基礎的函式，另外也有嘗試在DataFrame與RDD之間做轉換。
* 作業系統：CentOS-7 
* spark版本:2.2.0
* python版本:3.6.2
* IDE環境:jupyter

### 開啟SparkSession進入點

首先要來創建spark的entry point。從spark2.0開始後，不再使用SparkContext()來創建entry point，而是使用SparkSession來進行後續spark的編程，SparkContext, SQLContext等則被封裝在SparkSession裡，還是可以呼叫的。

	import findspark
	findspark.init()
	import pyspark
	ss = pyspark.sql.SparkSession.builder \
    	.master("local[1]") \
    	.appName("KC_house_price") \
    	.getOrCreate()
	ss
	print('ss type:',type(ss))

`spark.dataframe`可以透過`spark.session.createDataFrame`來創建。資料來源可以是下列三種：
1. RDD
2. list
3. pandas.DataFrame

下面是透過pandas.DataFrame來轉換成spark.dataframe。  
我們先準備pandas的DataFrame：

	import pandas as pd
	pddf = pd.read_csv('kc_house_data.csv')
	pddf.head(3)

有了pandas.DataFrame的資料後，接著我們就可以把pandas.DataFrame的資料轉成spark.DataFrame：

	spdf = ss.createDataFrame(pddf) # 從pd.dataframe匯入資料
	print('pydf type:',type(spdf))

從上面的print可以看到`spdf`的型態為`pyspark.sql.dataframe.DataFrame`。  
再來就是使用pyspark.DataFrame的函式來對資料進行操作：

	 # show DataFrame的schema
	spdf.printSchema()

	# 查詢columns name
	spdf.columns  
	# 呈現前n列(row)的資料
	spdf.show(n=5) 

	# select多個欄位(columns) 共有三種呼叫方是
	spdf.select((spdf.price/spdf.sqft_living).alias('SFP'), 'long', spdf['lat']) \
    	.show(n=5)

	# 條件過濾
	spdf.select((spdf.price/spdf.sqft_living).alias('SFP'), 'view') \
	    .filter(spdf.view >= 4) \
	    .show(n=5)

	# 分組計算
	spdf.groupBy(spdf.waterfront).count().show()

	# 排序
	spdf.select((spdf.price/spdf.sqft_living).alias('SFP'), 'condition') \
	    .sort(spdf['condition'].desc(), (spdf.price/spdf.sqft_living).asc()) \
	    .show(n=5)

此外，spark.DataFrame再經過轉換後，也可以使用SQL語句來選取資料：

	spdf.createOrReplaceTempView("pydf_table") # 將spark.dataframe轉成temporary view，之後就可以用SQL語法下指令
	df2 = ss.sql('''
		SELECT log(((price/sqft_living)+1)) as log_price, long, lat
		FROM pydf_table 
		LIMIT 5''')
	df2.show()

當然，spark RDD函式還是有保留的，DataFrame也可以很方便的和RDD進行轉換：

	rdd1 = pydf.rdd
	print('rdd1 type:', type(rdd1))

從上面的print可以看到`rdd1`的型態已經是`pyspark.rdd.RDD`。  
這時我們就可以使用pyspark.RDD的函式來對資料進行操作：

	# 計算資料筆數(row)
	print('RDD 資料筆數:', rdd1.count()) 
	# 只取第1筆資料
	rdd1.first() 

	# 'price'的統計資訊
	print('summary of price:', rdd1.map(lambda x:x['price']).stats()) 
	
	# 篩選'price'小於100000的資料並且計數
	price_under = rdd1.filter(lambda x: x['price'] < 100000).count() 
	print('price小於10萬筆數:', price_under)

	# 切分'date'欄位，只取日期出來
	date_rdd = rdd1.map(lambda x: x['date'].split("T")[0]) 
	# 呈現前六筆日期資料
	date_rdd.take(6)

以上就是pyspark.DataFrame和pyspark.RDD的一些函式的使用。  
最後離開時別忘記要將entry point給停止：

	ss.stop()
