# Canal Adapter
基于python canal-client实现的简易版本Canal适配器

***

## 功能
1. 从Canal Server接收数据，解析为SQL语句
    - 兼容DDL及DML操作
    - 记录断点来解决重复消费
    - 对Canal Server的断线重连
2. 将SQL语句同步至MySQL服务执行
    - 输出执行、错误日志

## 配置
1. 修改conf文件中CANAL_SERVER及MYSQL_SERVER配置


## 运行
1. 安装依赖
    - 执行 `pip3 install -r requirements.txt`

2. 启动Canal Server

3. 运行适配器
    - 执行 `python3 canal_adapter.py`