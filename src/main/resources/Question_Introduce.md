中国大数据算法大赛京东赛区——京东JDATA算法大赛决赛排行榜 [查看更多 >>](http://group.jd.com/thread/20000811/26275293/20002432.htm)

**01/****评价指标**

本赛题的评价指标分为用户评价和用户下单日期评价两部分：
（1）用户评价
![upfile](http://upload-images.jianshu.io/upload_images/14634833-73606913f1d38955.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

其中，o<sub>i </sub>表示选手预测的第 i 个用户的正确标志，当预测正确时 o<sub>i</sub>=1，否则 o<sub>i</sub>=0。N为选手提交的记录数。
（2）用户下单日期评价
![upfile](http://upload-images.jianshu.io/upload_images/14634833-266e8d0bd1370155.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

其中，U<sub>r</sub> 为答案用户集合，d<sub>u</sub> 表示用户 u 的预测日期与真实日期之间的距离。

本赛题，选手提交结果的得分由以下表达式确定:
![upfile](http://upload-images.jianshu.io/upload_images/14634833-d87e631cb6778dac.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

其中：α=0.4。

**02/****数据描述**

我们将提供如下数据表：
表1：SKU基本信息表（jdata_sku_basic_info）

![upfile](http://upload-images.jianshu.io/upload_images/14634833-b72d3e27e03acccb.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

表2：用户基本信息表（jdata_user_basic_info）

![upfile](http://upload-images.jianshu.io/upload_images/14634833-333f9a31e5168613.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

表3：用户行为表（jdata_user_action）

![upfile](http://upload-images.jianshu.io/upload_images/14634833-6bba5a31db268d09.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

表4：用户订单表（jdata_user_order）

![upfile](http://upload-images.jianshu.io/upload_images/14634833-9c61a04fd392c329.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

表5：评论分数数据表（jdata_user_comment_score）

![upfile](http://upload-images.jianshu.io/upload_images/14634833-f6c33620005bc081.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

数据来源说明如下：

![upfile](http://upload-images.jianshu.io/upload_images/14634833-c491a768f57ae15f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

![upfile](http://upload-images.jianshu.io/upload_images/14634833-6815cd656cdb7676.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

温馨提示：如需下载赛题数据，请先登录并完成报名参赛。

**03/任务描述**

参赛者需要根据赛题方提供的数据（用户基本信息、SKU基本信息、用户行为信息、用户下单信息及评价信息），自行设计数据处理相关操作、训练模型、预测未来1个月内最有可能购买目标品类的用户，并预测他们在考察时间段内的首次购买日期。
注：考察时间段，A榜为2017-05-01到2017-05-31，B榜为2017-09-01到2017-09-30。

**04/提交作品要求**

提交的CSV文件要求如下：
1\. UTF-8无BOM格式编码；
2\. 第一行为字段名，即：user_id（用户标识）, pred_date（预测的购买日期）；
3\. 结果必须按用户购买概率从大到小排序；
4\. 结果中不得存在重复的user_id，否则无效；
5\. 结果必须包含50000条记录（不包括字段名），否则无效；
6\. 结果user_id必须在用户基本信息表（jdata_user_basic_info）中, 否则无效。
提交结果示例如下图：
![upfile](http://upload-images.jianshu.io/upload_images/14634833-95e640f4fa28deff.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
