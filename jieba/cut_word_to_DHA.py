#### cut words by everyday ####
# 設定spark session 環境變數 spark home
# 詳細 ?findspark.init
import findspark
findspark.init('/usr/local/spark')
# 載入必要module
import sys
import re
import jieba
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.sql import types

# 設定 hadoop 路徑
spark_url = "mesos://zk://your_ip_1:post1,your_ip_2:port2/mesos"
hdfs_path = "hdfs://your_ip"
coresMax = "6"
executorMem = "6g"
appName = "cut words script"

def WriteAndRepartition(dataFrame, tempPath, finalPath, source = "csv"):
    dataFrame.write.format(source).mode("overwrite").option("compression", "gzip").save(path=tempPath)
    temp_df = ss.read.format(source).load(path=tempPath)
    temp_df.write.format(source).mode("overwrite").option("compression", "gzip").save(path=finalPath)

def cut_text(df, input_colnames, output_colnames, withTitle=False):
    df = df.toDF(*input_colnames)
    if withTitle:
        df = df.where("LENGTH(text) > 5 AND text IS NOT NULL") \
            .select(fn.lit(date).alias("date"), *output_colnames[1:-1],
                    cut_udf(fn.regexp_replace(fn.concat_ws("。", 'title', 'text'), "\\\\\\\\n", "。")).alias('words'))
    else:
        df = df.where("LENGTH(text) > 5 AND text IS NOT NULL") \
            .select(fn.lit(date).alias("date"), *output_colnames[1:-1],
                    cut_udf(fn.regexp_replace('text', "\\\\\\\\n", "。")).alias('words'))
            # trim 字串處理
    df = df.where("words[0] IS NOT NULL")
    return df

ss = SparkSession.builder \
    .appName( appName ) \
    .master( spark_url ) \
    .config( "spark.cores.max", coresMax ) \
    .config( "spark.executor.memory", executorMem ) \
    .config( "spark.driver.memory", "24g" ) \
    .config( "spark.driver.maxResultSize", "16g" ) \
    .getOrCreate()

ruten_colnames = [
    ["PID", "website", "categroy", "url", "title", "price", "text", "createTime"],
    ['date', 'website', 'categroy', 'url', 'title', 'words'],
]
ptt_colnames = [
    ["DID", "website", "categroy", "url", "title", "text", "createTime", "push", "crawlerTime"],
    ['date','website', 'categroy', fn.lit('hot').alias('isTrain'), 'url', 'title', 'words'],
    ['date','website', 'categroy', fn.lit('train').alias('isTrain'), 'url', 'title', 'words'],
]
afp_publisher_colnames = [
    ['ID', 'website', 'appID', 'url', 'title', 'text', 'crawler_time'],
    ['date', 'website', 'appID', 'url', 'title', 'words'],
]
ata_container_colnames = [
    ['ID', 'website', 'containerID', 'url', 'title', 'text', 'crawler_time'],
    ['date', 'website', 'containerID', 'url', 'title', 'words'],
]
afp_landingpage_colnames = [
    ['ID', 'website', 'campaignMainId', 'url', 'title', 'text', 'crawler_time'],
    ['date', 'website', 'campaignMainId', 'url', 'title', 'words'],
]

if len( sys.argv ) != 2:
    print( "the number of argsInfo is error..." )
else:
    date = sys.argv[1]
    # 匯入停用詞
    stopWord_set = set()
    stopWords_path = '/data/stop_word_test1800.txt'
    with open( stopWords_path, 'r', encoding='utf-8') as stopwords:
        for stopword in stopwords:
            stopWord_set.add( stopword.strip( '\n' ) )

    # 修改 jieba 的斷詞判定，加入空白和單引號( ,')作為一個詞
    # re.findall, re.sub, re.
    # jieba.re_han_default = re.compile("([\u4E00-\u9FD5a-zA-Z0-9+#&\._% ']+)", re.U)
    # 設定 UDF 使用 jieba 斷詞，並依停用詞來篩選。最後輸出格式為 array, 內容為 str
    def cut_stopWord(w):
        l = []
        w = w.lower()
        seg = jieba.cut(w)
        for word in seg:
            if word not in stopWord_set:
                if re.search("[\u4e00-\u9fa5a-z]", word):
                    if not re.search( "^\d+(ml|cm|mm|km)", word):
                        if len(word) > 1:
                            l.append(word)
        return l
    cut_udf = fn.udf(cut_stopWord, types.ArrayType(types.StringType()))

    #### ruten
    ruten_df = ss.read.csv(
        path = hdfs_path + "/adbert_data_house/daily_observation/attribute/" + date + "/corpus_ruten",
        sep = "\t")
    if ruten_df.rdd.isEmpty():
        print("There is no ruten data on " + date)
    else:
        ruten_df_cut = cut_text(ruten_df, ruten_colnames[0], ruten_colnames[1], withTitle=True)
        WriteAndRepartition(
            dataFrame = ruten_df_cut,
            tempPath = hdfs_path + "/corpus/temp/temp_data_frame_01",
            finalPath = hdfs_path + "/daily_observation/attribute/" \
                        + date + "/corpus_ruten_cut",
            source = "parquet")

    #### ptt
    ptt_hot_flog = True
    ptt_train_flog = True
    ptt_hot_df = ss.read.csv(
        path = hdfs_path + "/daily_observation/attribute/" + date + "/corpus_ptt_hot",
        sep = "\t")
    if ptt_hot_df.rdd.isEmpty():
        print("There is no ptt_hot data on " + date)
        ptt_hot_flog = False
    else:
        ptt_hot_df_cut = cut_text(ptt_hot_df, ptt_colnames[0], ptt_colnames[1])
    #
    ptt_train_df = ss.read.csv(
        path = hdfs_path + "/daily_observation/attribute/" + date + "/corpus_ptt_train",
        sep = "\t")
    if ptt_train_df.rdd.isEmpty():
        print("There is no ptt_train data on " + date)
        ptt_train_flog = False
    else:
        ptt_train_df_cut = cut_text(ptt_train_df, ptt_colnames[0], ptt_colnames[2])
    #
    # rbind 後寫出
    if ptt_hot_flog & ptt_train_flog:
        ptt_df_cut = ptt_hot_df_cut.union(ptt_train_df_cut)
        WriteAndRepartition(
            dataFrame = ptt_df_cut,
            tempPath = hdfs_path + "/corpus/temp/temp_data_frame_01",
            finalPath = hdfs_path + "/daily_observation/attribute/" \
                        + date + "/corpus_ptt_cut",
            source = "parquet")
    elif ptt_train_flog & (not ptt_hot_flog):
        WriteAndRepartition(
            dataFrame = ptt_train_df_cut,
            tempPath = hdfs_path + "/corpus/temp/temp_data_frame_01",
            finalPath = hdfs_path + "/daily_observation/attribute/" \
                        + date + "/corpus_ptt_cut",
            source = "parquet")
    elif ptt_hot_flog & (not ptt_train_flog):
        WriteAndRepartition(
            dataFrame = ptt_hot_df_cut,
            tempPath = hdfs_path + "/corpus/temp/temp_data_frame_01",
            finalPath = hdfs_path + "/daily_observation/attribute/" \
                        + date + "/corpus_ptt_cut",
            source = "parquet")
    else:
        print("ptt train and hot both empty")

    #### AFPpublisher
    afp_publiser_df = ss.read.csv(
        path = hdfs_path + "/daily_observation/attribute/" + date + "/corpus_afp_publisher",
        sep = "\t")
    if afp_publiser_df.rdd.isEmpty():
        print("There is no AFP publisher on " + date)
    else:
        afp_publiser_df_cut = cut_text(afp_publiser_df, afp_publisher_colnames[0], afp_publisher_colnames[1])
        WriteAndRepartition(
            dataFrame = afp_publiser_df_cut,
            tempPath = hdfs_path + "/corpus/temp/temp_data_frame_01",
            finalPath = hdfs_path + "/daily_observation/attribute/" \
                        + date + "/corpus_afp_publisher_cut",
            source = "parquet")

    #### ATAcontainer
    ata_container_df = ss.read.csv(
        path = hdfs_path + "/adbert_data_house/daily_observation/attribute/" + date + "/corpus_ata_container",
        sep = "\t")
    if ata_container_df.rdd.isEmpty():
        print("There is no ATA container on " + date)
    else:
        ata_container_df_cut = cut_text(ata_container_df, ata_container_colnames[0], ata_container_colnames[1])
        WriteAndRepartition(
            dataFrame = ata_container_df_cut,
            tempPath = hdfs_path + "/corpus/temp/temp_data_frame_01",
            finalPath = hdfs_path + "/daily_observation/attribute/" \
                        + date + "/corpus_ata_container_cut",
            source = "parquet")

    #### AFPadvertiser
    afp_landingpage_df = ss.read.csv(
        path = hdfs_path + "/adbert_data_house/daily_observation/attribute/" + date + "/corpus_afp_advertiser",
        sep = "\t")
    if afp_landingpage_df.rdd.isEmpty():
        print("There is no landing page on " + date)
    else:
        afp_landingpage_df_cut = cut_text(afp_landingpage_df, afp_landingpage_colnames[0], afp_landingpage_colnames[1])
        WriteAndRepartition(
            dataFrame = afp_landingpage_df_cut,
            tempPath = hdfs_path + "/corpus/temp/temp_data_frame_01",
            finalPath = hdfs_path + "/daily_observation/attribute/" \
                        + date + "/corpus_afp_advertiser_cut",
            source = "parquet")
ss.stop()
