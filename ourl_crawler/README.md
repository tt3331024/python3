ourl crawler
==========

爬取 landing page 的網頁資訊，提供給短網址超聯結呈現縮圖資訊使用。  
在 GCP ourl 專案上部屬，使用 Cloud Functions 服務，使用 Https 作為觸發條件。  
由於會有機會用到 headless browser，因此分配 RAM 至少需開到 1G。  
python 版本為 3.8 版。 

### 所需套件 ###
參見 `./requirements.txt`。

### 爬蟲規則 ###

爬取四個項目並回傳:
1. 網站 title
2. 網站描述
3. 網頁圖示
4. 網站名稱

##### 1. 網站 title #####

`og:title > head title > 'none'`

##### 2. 網站描述 #####

`og:description > description > h1 > h2 > h3 > 網站title`

##### 3. 網頁圖示 #####

`og:image > link[rel|='apple-touch-icon'] > link[rel*='icon'] > img > 'none'`

##### 4. 網站名稱 #####

`og:site_name > 網站title`

當網頁使用 SPA 架構，requests 無法取得 HTTP 200 或是 get 不到 title 時，會改使用 pyppeteer 以 headless browser 方式來獲取資訊。

### 使用說明 ###

* Input
帶有 http:// or https:// 的網址。
```
https://www.yoxi.app/promotion/93
```
* Return
帶有 title, desc, image, name 4個 key 值的 json 格式。
```
{
  "desc": "yoxi企簽好評加碼「企業乘車月結專案」為企業主節省60%以上核銷成本為同仁帶來省時省力的洽公體驗現在加入享有趟趟9折優惠！ 活動日期： 2021/9/1~2021/12/31活動內容： 期間搭乘使用企簽支付，趟趟享有9折優惠適用對象： 已加入的企簽客戶或期間新加入的企簽客戶(欲加入企業請洽02-8...",
  "image": "https://www.yoxi.app/api/operator/web/promotion/img/WPFileName_93.png",
  "name": "yoxi",
  "title": "yoxi企客好評再加碼 月結趟趟9折優惠！"
}
```

### 版本更新 ###
* version 1.0.0 (2021/09/28)  
  創建 script。