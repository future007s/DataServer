## 功能
提供接口，接收数据，然后插入或者更新数据库
## 依赖
数据库使用的new_energy_server的，需要在new_energy_server的db服务正常运行
## 使用方法：
1. 创建镜像并启动容器
    - 创建项目目录
    - 目录下执行`docker-compose build && docker-compose up -d`（存在问题）
    - 之后就可以发送请求了
2. 挂载数据
    - nginx挂载nginx的日志
    - mysql挂载mysql的数据
    - web_logs挂载web服务的日志
## 问题
- 首次启动docker-compose，不存在数据库的时候，迁移会报错，连不上数据库，restart web服务后才能正常创建表
    - 可能原因是数据库尚未启动完全
    - 暂时解决办法：启动容器后再重启一次web服务
        - docker-compose up -d
        - docker-compose restart web
